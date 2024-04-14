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
    create_codeql_database,
    run_codeql_analyze,
    clone_repo,
    log_queries,
    extract_result,
    parse_json_values,
    parse_csv_results,
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

def render_templates(logger, target_path, data):
    shutil.copyfile("queries/qlpack.yml", os.path.join(target_path, "qlpack.yml"))
        
    for d in data:
        template_file = d.pop('template_file')
        rendered_content = templates[template_file].render(**d)
        
        with open(os.path.join(target_path, template_file), "w") as file:
            file.write(rendered_content)

    logger.debug(f"Rendered to templates to {target_path}")


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

def process_batch(logger, repo_url, batch):
    with create_tmp_cache() as query_dir:
        render_templates(logger, query_dir, batch)
        
        
            

def process_batches(logger, repo_url, db_dir, targets, batch_size):
    batch_methods = []
    batch_testcases = {}

    for i, method_test in targets.groupby("focal_method.cm_signature"):
        method_cm_sig = i
        method_class = method_test["focal_class.identifier"].iloc[0]
        method_name = extract_method_name(method_cm_sig)
        batch_methods.append({
            "name": i,
            "template_file": "count_stats.ql",
            "class_name": method_class,
            "method_name": method_name,
            "relative_path": method_test['focal_class.file'].iloc[0]
        })

        for j, test_case in method_test.iterrows():
            test_cm_sig = test_case["test_case.cm_signature"]
            method_name = extract_method_name(test_cm_sig)
            test_class = test_case["test_class.identifier"]
            
            batch_testcases.setdefault(i, [])
            batch_testcases[i].append({
                "template_file": "count_test_stats.ql",
                "class_name": test_class, "method_name": method_name, 
                "relative_path": test_case['test_class.file']
            })

    batch = []
    method_index = 0
    while len(batch) < batch_size:
        method_params = batch_methods[method_index]
        method_id = method_params.pop('name')
        batch.append(method_params)
        batch += batch_testcases[method_id]
        method_index += 1
    
    logger.info(f"Processing batch of size {len(batch)}")
    
        
            
            


def process_repo(repo_url, ddf):
    logger = setup_logging(repo_url)

    query_env = lmdb.open("query_cache")
    stats_env = lmdb.open("stats")
    targets = ddf[ddf["repository.url"] == repo_url]

    codeql_db = make_codeql_database(logger, repo_url, targets)

    if codeql_db is None:
        DBUtils.update_stats(stats_env, success=False, level="repo", name=repo_url)
        return

    logger.debug(f"Found {len(targets)} methods inside repository {repo_url}")

    for i in range(0, len(targets), BATCH_SIZE):
        batch_targets = targets.iloc[i:i+BATCH_SIZE]
        method_results, test_results = process_batch(logger, repo_url, codeql_db, batch_targets, BATCH_SIZE)

        for method_sig, results in method_results.items():
            for result in results:
                DBUtils.add_item(query_env, method_sig, result)
            DBUtils.update_stats(stats_env, success=True, level="method", name=method_sig)

        for test_sig, results in test_results.items():
            for result in results:
                query_key = f"{result['focal_method.cm_signature']}_{test_sig}"
                DBUtils.add_item(query_env, query_key, result)
            DBUtils.update_stats(stats_env, success=True, level="testcase", name=test_sig)

    DBUtils.update_stats(stats_env, success=True, level="repo", name=repo_url)
    logger.warning("Cleaning up remaining files...")
    shutil.rmtree(codeql_db)


def main():

    def process_repo_task(repo_url, ddf):
        result = process_repo(repo_url, ddf)
        return True

    task_args = [(repo_url, partitioned_df) for i, repo_url in enumerate(repositories)]
    num_threads = 1

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