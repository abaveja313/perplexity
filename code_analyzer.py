from pathlib import Path
import os
from tqdm import tqdm
import traceback
import json
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import logging
import pprint
import Levenshtein
import subprocess
import re
import tempfile
import shutil
import lmdb
import csv
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
    get_cache_creator,
)


NUM_PARTITIONS = 2
PARTITION_INDEX = 0
BATCH_SIZE = 100

print("Reading DF")
df = get_or_create_df(df_path="/sailhome/abaveja/df.feather")
print("Read DF")

partitioned_df = get_partition(df, NUM_PARTITIONS, PARTITION_INDEX)
print(f"Size of Partition: {len(partitioned_df)}")

repositories = partitioned_df["repository.url"].unique()
print(f"Found {len(partitioned_df)} unique testcases")

create_tmp_cache = get_cache_creator("/dev/shm/cache")

templates = get_templates()


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


def make_codeql_database(logger, repo_url, ddb):
    logger.warning(f"Cloning repository: {repo_url}")

    with tempfile.TemporaryDirectory(dir="/dev/shm/cache") as clone_dir:
        try:
            clone_repo(repo_url, clone_dir)
            include = get_include(ddb)
            logger.debug(f"Includes: {include}")
            remaining, deleted = clean_repo(clone_dir, include)
            logger.info(f"Removed {deleted} files... {remaining} remaining.")
            if remaining == 0:
                logger.error(
                    f"An unexpected error occurred... no files are left in the {repo_url}"
                )

            db_dir = tempfile.mkdtemp(dir='/dev/shm/cache')
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



def run_query(logger, repo_url, db_dir, sig, class_name, rel_path, stats_query, log_query):
    with create_tmp_cache() as query_dir:
        method_name = extract_method_name(sig)
        render_templates(
            logger, query_dir, method_name=method_name, class_name=class_name, relative_path=rel_path
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

            logger.info(f"Using result with lev-dist {min_dist}: {min_result}")
            return min_result

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


def process_method(logger, repo_url, db_dir, query_db, method_id, method_test):
    method_cm_sig = method_test["focal_method.cm_signature"][0]
    result = DBUtils.item_exists(query_db, method_cm_sig)

    if result:
        logger.debug(f"Using cache for method {method_cm_sig}")
        return result

    method_class = method_test["focal_class.identifier"][0]
    if method_id == method_class:
        logger.info(f"Skipping constructor {method_id}")
        return "constructor"

    out = run_query(
        logger,
        repo_url,
        db_dir,
        method_cm_sig,
        method_class,
        method_test['focal_class.file'][0],
        "count_stats.ql",
        "log_functions.ql",
    )
    DBUtils.add_item(query_db, method_cm_sig, out)
    return out


def process_testcase(logger, repo_url, db_dir, query_db, test_case):
    test_cm_sig = test_case["test_case.cm_signature"]
    query_key = test_case["focal_method.cm_signature"] + "_" + test_cm_sig

    result = DBUtils.item_exists(query_db, query_key)
    if result:
        logger.debug(f"Using cache for test {test_cm_sig}")
        return result

    test_class = test_case["test_class.identifier"]

    out = run_query(
        logger,
        repo_url,
        db_dir,
        test_cm_sig,
        test_class,
        test_case['test_class.file'],
        "count_test_stats.ql",
        "log_functions.ql",
    )
    DBUtils.add_item(query_db, query_key, out)
    return out

def process_repo(repo_url, ddf):
    logger = setup_logging(repo_url)

    query_env = lmdb.open("/matx/u/abaveja/query_cache")
    stats_env = lmdb.open("/matx/u/abaveja/stats")
    targets = ddf[ddf["repository.url"] == repo_url]

    codeql_db = make_codeql_database(logger, repo_url, targets)

    if codeql_db is None:
        DBUtils.update_stats(stats_env, success=False, level="repo", name=repo_url)
        return

    methods = targets.groupby("focal_method.identifier").agg(list)

    logger.debug(f"Found {len(methods)} methods inside repository {repo_url}")

    succeeded = False

    for method_id, method_test in methods.iterrows():
        method_cm_sig = method_test["focal_method.cm_signature"][0]
        logger.warning(f"Processing method {method_cm_sig}")
        method_stats = process_method(
            logger, repo_url, codeql_db, query_env, method_id, method_test
        )
        if method_stats is None:
            DBUtils.update_stats(
                stats_env, success=False, level="method", name=method_cm_sig
            )
            continue

        test_cases = targets[targets["focal_method.cm_signature"] == method_cm_sig]
        for i, test_case in test_cases.iterrows():
            test_cm_sig = test_case["test_case.cm_signature"]
            logger.warning(f"Processing testcase {test_cm_sig}")
            testcase_stats = process_testcase(
                logger, repo_url, codeql_db, query_env, test_case
            )
            DBUtils.update_stats(
                stats_env,
                success=testcase_stats is not None,
                level="testcase",
                name=test_cm_sig,
            )

        DBUtils.update_stats(
            stats_env, success=True, level="method", name=method_cm_sig
        )
    DBUtils.update_stats(stats_env, success=True, level="repo", name=repo_url)
    logger.warning("Cleaning up remaining files...")
    shutil.rmtree(codeql_db)


def main():

    def process_repo_task(repo_url, ddf):
        result = process_repo(repo_url, ddf)
        return True

    task_args = [(repo_url, partitioned_df) for i, repo_url in enumerate(repositories)]
    num_threads = 9

    print("Computing results in parallel...")

    pbar = tqdm(
        total=len(task_args),
        desc="Processing repo results",
        unit="repo",
    )

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(process_repo_task, *args) for args in task_args]

        for future in as_completed(futures):
            result = future.result()
            pbar.update(1)

    pbar.close()


if __name__ == "__main__":
    main()