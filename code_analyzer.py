import os
from tqdm import tqdm
import traceback
from concurrent.futures import ProcessPoolExecutor, wait, FIRST_COMPLETED
import Levenshtein
import subprocess
import signal
import re
import tempfile
import shutil
import multiprocessing
import lmdb
from dataset_utils import (
    setup_logging,
    parse_table,
    create_codeql_database,
    run_codeql_query,
    clone_repo,
    log_queries,
    parse_json_values,
    DBUtils,
    clean_repo,
    get_or_create_df,
    get_partition,
    get_templates,
    convert_unix_to_pst,
    NoProgressMadeException,
    no_progress_handler,
)
import time
from datetime import timedelta
import psutil
import argparse
import threading
import logging


templates = get_templates()


def print_alert(*args):
    print("************", *args, "**********")


def extract_method_name(input_string):
    match = re.search(r"\.(\w+)\(", input_string)
    if match:
        return match.group(1)


def get_include(repos):
    include_set = set()

    for i, test in repos.iterrows():
        focal_class_path = test["focal_class.file"]
        test_class_path = test["test_class.file"]
        include_set.add(focal_class_path)
        include_set.add(test_class_path)
    return list(include_set)


def render_templates(logger, target_path, **kwargs):
    shutil.copyfile("queries/qlpack.yml", os.path.join(target_path, "qlpack.yml"))

    for template_file, template in templates.items():
        rendered_content = template.render(**kwargs)
        with open(os.path.join(target_path, template_file), "w") as file:
            file.write(rendered_content)

    logger.debug(f"Rendered {len(templates)} templates to {target_path}")


def make_codeql_database(logger, repo_url, ddb, args, last_updated):
    logger.warning(f"Cloning repository: {repo_url}")

    with tempfile.TemporaryDirectory(dir=args.repo_clone_dir) as clone_dir:
        try:
            clone_repo(logger, repo_url, clone_dir)
            DBUtils.register_work(repo_url=repo_url, last_updated=last_updated)
            include = get_include(ddb)
            logger.debug(f"Includes: {include}")
            remaining, deleted = clean_repo(clone_dir, include)
            logger.info(f"Removed {deleted} files... {remaining} remaining.")
            if remaining == 0:
                logger.error(
                    f"An unexpected error occurred... no files are left in the {repo_url}"
                )

            db_dir = tempfile.mkdtemp(dir=args.database_dir)
            logger.info(f"Making Database from {clone_dir} to {db_dir}")
            create_codeql_database(clone_dir, db_dir)
            return db_dir

        except Exception as e:
            logger.error(f"Error processing repository: {repo_url}")
            logger.error(traceback.format_exc())
            if isinstance(e, subprocess.CalledProcessError):
                logger.error("Error:")
                logger.error(f"stdout: {e.stdout}")
                logger.error(f"stderr: {e.stderr}")
            logger.error(f"Clone directory structure: {os.listdir(clone_dir)}")
            return None


def run_query(
    logger, repo_url, db_dir, sig, class_name, rel_path, stats_query, log_query, args
):
    with tempfile.TemporaryDirectory(dir=args.query_dir) as query_dir:
        method_name = extract_method_name(sig)
        render_templates(
            logger,
            query_dir,
            method_name=method_name,
            class_name=class_name,
            relative_path=rel_path,
        )
        logger.debug(
            f"Running CodeQL queries for repository: {repo_url}, method: {sig}, class: {class_name}"
        )

        try:
            stats_output = run_codeql_query(
                os.path.join(query_dir, stats_query), db_dir
            )
            results = parse_table(stats_output.splitlines())
            min_dist = float("inf")
            min_result = None
            for result in results:
                parsed_result = parse_json_values(result)
                dist = Levenshtein.distance(sig, parsed_result["gsig"])
                if dist < min_dist:
                    min_dist = dist
                    min_result = parsed_result

            logger.info(
                f"Using result with lev-dist {min_dist}: {min_result}, repo={repo_url}, method={sig}, class={class_name}"
            )
            return min_result

        except NoProgressMadeException:
            raise
        except Exception as e:
            logger.error(traceback.format_exc())
            log_queries(logger, query_dir)
            if isinstance(e, subprocess.CalledProcessError):
                logger.error(f"stdout: {e.stdout}")
                logger.error(f"stderr: {e.stderr}")
            log_functions_out = run_codeql_query(
                os.path.join(query_dir, log_query), db_dir
            )
            logger.error(f"Log functions output: {log_functions_out}")
            return None


def process_method(logger, repo_url, db_dir, query_db, method_id, method_test, args):
    method_cm_sig = method_test["focal_method.cm_signature"][0]
    method_key = repo_url + "\0" + method_cm_sig
    result = DBUtils.item_exists(query_db, method_cm_sig)

    if result:
        logger.debug(f"Using cache for method {method_cm_sig}")
        return result, True

    method_class = method_test["focal_class.identifier"][0]
    if method_id == method_class:
        logger.info(f"Skipping constructor {method_id}")
        return "constructor", False

    out = run_query(
        logger,
        repo_url,
        db_dir,
        method_cm_sig,
        method_class,
        method_test["focal_class.file"][0],
        "count_stats.ql",
        "log_functions.ql",
        args,
    )
    out['t'] = 'method'
    DBUtils.add_item(query_db, method_key, out)
    return out, False


def process_testcase(logger, repo_url, db_dir, query_db, test_case, args):
    test_cm_sig = test_case["test_case.cm_signature"]
    query_key = repo_url + "\0" + test_case["focal_method.cm_signature"] + "\0" + test_cm_sig

    result = DBUtils.item_exists(query_db, query_key)
    if result:
        logger.debug(f"Using cache for test {test_cm_sig}")
        return result, True

    test_class = test_case["test_class.identifier"]

    out = run_query(
        logger,
        repo_url,
        db_dir,
        test_cm_sig,
        test_class,
        test_case["test_class.file"],
        "count_test_stats.ql",
        "log_functions.ql",
        args,
    )
    out['t'] = 'testcase'
    DBUtils.add_item(query_db, query_key, out)
    return out, False


def process_repo(logger, repo_url, targets, last_updated, args):
    query_env = lmdb.open(args.results_db, map_size=int(1e9))  # 1GB of space
    stats_env = lmdb.open(args.stats_db)
    update_kws = dict(repo_url=repo_url, last_updated=last_updated)
    db_created = False
    try:
        if DBUtils.item_exists(query_env, repo_url):
            DBUtils.update_stats(
                query_env,
                success=True,
                level="cached_repo",
                name=repo_url,
                **update_kws,
            )
            return

        codeql_db = make_codeql_database(logger, repo_url, targets, args, last_updated)
        DBUtils.register_work(repo_url, last_updated)
        
        db_created = True

        if codeql_db is None:
            DBUtils.update_stats(
                stats_env, success=False, level="repo", name=repo_url, **update_kws
            )
            return

        methods = targets.groupby("focal_method.identifier").agg(list)

        logger.debug(f"Found {len(methods)} methods inside repository {repo_url}")

        for method_id, method_test in methods.iterrows():
            method_cm_sig = method_test["focal_method.cm_signature"][0]
            logger.warning(f"Processing method {method_cm_sig}")
            method_stats, cached = process_method(
                logger, repo_url, codeql_db, query_env, method_id, method_test, args
            )
            if method_stats is None:
                if not cached:
                    DBUtils.update_stats(
                        stats_env,
                        success=False,
                        level="method",
                        name=method_cm_sig,
                        **update_kws,
                    )
                else:
                    DBUtils.update_stats(
                        stats_env,
                        success=True,
                        level="cached_method",
                        name=method_cm_sig,
                        **update_kws,
                    )
                continue

            DBUtils.update_stats(
                stats_env,
                success=True,
                level="method",
                name=method_cm_sig,
                **update_kws,
            )
        DBUtils.update_stats(
            stats_env, success=True, level="repo", name=repo_url, **update_kws
        )
        DBUtils.add_item(env=query_env, key=repo_url, results=dict(completed=True, t='repo'))

    finally:
        logger.warning("Closing environments...")
        query_env.close()
        stats_env.close()
        logger.warning("Cleaning up remaining files...")
        try:
            shutil.rmtree(codeql_db)
        except:
            pass


def process_repo_task(repo_url, ddf, last_updated, process_pid, retry_count, args):
    logger = setup_logging(repo_url=repo_url, attempt=retry_count[repo_url])
    try:
        signal.signal(signal.SIGUSR1, no_progress_handler)
        current_pid = os.getpid()
        logger.info(
            f"Starting task {repo_url} on {current_pid} (attempt #{retry_count[repo_url]})"
        )
        process_pid[repo_url] = current_pid
        last_updated[repo_url] = time.time()
        result = process_repo(logger, repo_url, ddf, last_updated, args)
        logger.info(f"Removing {repo_url} from managed caches")
        last_updated.pop(repo_url)
        process_pid.pop(repo_url)
        return True

    except NoProgressMadeException:
        logger.error(f"Restarting task {repo_url}.")
        return None

    except Exception as e:
        logger.exception(f"Error processing repository: {repo_url}")
        return e


def monitor_inactivity(
    futures,
    future_repo_mapping,
    last_updated,
    process_pid,
    retry_count,
    partitioned_df,
    max_retries,
    executor,
    args,
):
    while True:
        time.sleep(30)  # Check every 30 secs
        print_alert("Checking processes for inactivity...")
        try:
            target = {url: convert_unix_to_pst(last_updated[url]) for url in last_updated}
            DBUtils.sync_data(target, "/matx/u/abaveja/last_updated.json")

            for future in futures:
                repo_url = future_repo_mapping[future]
                if not future.running() or not repo_url in target:
                    continue

                repo_url = future_repo_mapping[future]
                diff = time.time() - last_updated[repo_url]
                if diff > timedelta(minutes=6).total_seconds():
                    ddf = partitioned_df[partitioned_df["repository.url"] == repo_url]
                    retry_count[repo_url] += 1
                    print_alert("Killing Worker")
                    os.kill(process_pid[repo_url], signal.SIGUSR1)
                    last_updated.pop(repo_url)
                    process_pid.pop(repo_url)
                    time.sleep(3)
                    
                    if retry_count[repo_url] < max_retries:
                        new_future = executor.submit(
                            process_repo_task,
                            repo_url,
                            ddf,
                            last_updated,
                            process_pid,
                            retry_count,
                            args,
                        )
                        futures.append(new_future)
                        future_repo_mapping[new_future] = repo_url
                    else:
                        print_alert(
                            f"Max retries reached for repository: {repo_url}. Skipping."
                        )
                        futures.remove(future)
        except Exception as e:
            import traceback
            traceback.print_exc()
            continue

def main():
    parser = argparse.ArgumentParser(description="Script to process repositories.")

    parser.add_argument(
        "-n", "--num-partitions", type=int, default=4, help="Number of partitions"
    )
    parser.add_argument(
        "-i",
        "--partition-index",
        type=int,
        required=True,
        help="Index of the current partition",
    )
    parser.add_argument(
        "-s", "--shuffle", type=bool, help="Whether to shuffle the partition order"
    )
    parser.add_argument(
        "-b", "--batch-size", type=int, default=100, help="Batch size for processing"
    )
    parser.add_argument(
        "--df-path",
        type=str,
        default="/sailhome/abaveja/df.feather",
        help="Path to the DataFrame",
    )
    parser.add_argument(
        "--query-dir",
        type=str,
        default="/dev/shm/cache",
        help="Directory for query cache",
    )
    parser.add_argument(
        "--results-db",
        type=str,
        default="/scr/abaveja/query_cache",
        help="Directory for results DB",
    )
    parser.add_argument(
        "--stats-db",
        type=str,
        default="/matx/u/abaveja/stats_matx1",
        help="Directory for stats DB",
    )
    parser.add_argument(
        "--repo-clone-dir",
        type=str,
        default="/dev/shm/cache",
        help="Directory to clone repositories into",
    )
    parser.add_argument(
        "--database-dir",
        type=str,
        default="/scr/abaveja/repos",
        help="Directory to store CodeQL databases",
    )
    parser.add_argument(
        "--default-cpus",
        type=int,
        default=10,
        help="Default number of cores to use for a single job",
    )
    parser.add_argument(
        "--max-retries", type=int, default=3, help="Max retries for a job"
    )
    parser.add_argument(
        "--timeout", type=int, default=3600, help="Repository processing timeout"
    )

    args = parser.parse_args()

    print_alert("Reading DF")
    df = get_or_create_df(df_path=args.df_path)
    print_alert("Read DF")

    partitioned_df = get_partition(df, args.num_partitions, args.partition_index)
    if args.shuffle:
        print_alert("Shuffling...")
        partitioned_df = partitioned_df.sample(frac=1)

    print_alert(f"Size of Partition: {len(partitioned_df)}")
    repositories = partitioned_df["repository.url"].unique()
    print_alert(f"Found {len(repositories)} unique repositories")

    task_args = [
        (repo_url, partitioned_df[partitioned_df["repository.url"] == repo_url])
        for repo_url in repositories
    ]
    num_processes = int(os.environ.get("SLURM_CPUS_ON_NODE", args.default_cpus))
    print_alert(f"Using {num_processes} vCPUs for processing")
    max_retries = args.max_retries

    print_alert("Computing results in parallel...")
    pbar = tqdm(total=len(task_args), desc="Processing repo results", unit="repo")

    manager = multiprocessing.Manager()
    last_updated = manager.dict()
    process_pid = manager.dict()
    retry_count = manager.dict()
    future_repo_mapping = {}

    # Instantiate executor without a context manager
    executor = ProcessPoolExecutor(max_workers=num_processes)
    futures = []

    for targs in task_args:
        repo_url = targs[0]
        retry_count[repo_url] = 0
        future = executor.submit(
            process_repo_task, *targs, last_updated, process_pid, retry_count, args
        )
        futures.append(future)
        future_repo_mapping[future] = repo_url

    inactivity_thread = threading.Thread(
        target=monitor_inactivity,
        args=(
            futures,
            future_repo_mapping,
            last_updated,
            process_pid,
            retry_count,
            partitioned_df,
            max_retries,
            executor,
            args,
        ),
    )
    inactivity_thread.start()

    while futures:
        done, _ = wait(futures, return_when=FIRST_COMPLETED)

        for future in done:
            repo_url = future_repo_mapping[future]
            exception = future.exception()
            result = future.result()
            if result:
                pbar.update(1)
            else:
                print_alert(f"Error processing repository: {repo_url}")
                traceback.print_exception(
                    type(exception), exception, exception.__traceback__
                )

            futures.remove(future)

    inactivity_thread.join()

    # Explicitly shutdown the executor
    executor.shutdown()

    pbar.close()


if __name__ == "__main__":
    main()
