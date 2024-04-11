from pathlib import Path
import os
from tqdm import tqdm
import traceback
import json
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
import dask
import pprint
import Levenshtein
import re
from jinja2 import Environment, FileSystemLoader

from dataset_utils import setup_logging, is_valid_java_repo, create_codeql_database,\
    run_codeql_query, clone_repo, log_queries, extract_result, get_params,\
    parse_json_values, parse_table, DBUtils, clean_repo, get_or_create_df, be,\
    get_templates, get_cache_creator, BufferingHandler
    
    
NUM_PARTITIONS = 2
PARTITION_INDEX = 0

df = get_or_create_df(path='df.parquet.zip')

partitioned_df = get_partition(df, NUM_PARTITIONS, PARTITION_INDEX)
print(f"Size of Partition: {len(partitioned_df)}")

repositories = partitioned_df['repository.url'].unique()
print(f"Found {len(partitioned_df)} unique repositories")

create_tmp_cache = get_cache_creator('/dev/shm/cache')

templates = get_templates()

def extract_method_name(input_string): 
    match = re.search(r"\.(\w+)\(", input_string)
    if match:
        return match.group(1)
        
def get_include(repos):
    include_set = set()

    for i, test in repos.iterrows():
        focal_class_path = test['focal_class.file']
        test_class_path = test['test_class.file']
        # We want to analyze the focal class and all of the files in the same package
        include_set.add(os.path.dirname(focal_class_path))
        # Don't care about other tests since they are usually self-contained
        include_set.add(test_class_path)
    return list(include_set)

def render_templates(logger, target_path, **kwargs):
    shutil.copyfile("queries/qlpack.yml", os.path.join(target_path, "qlpack.yml"))
    
    for template_file, template in templates.items():
        rendered_content = template.render(**kwargs)
        with open(os.path.join(target_path, template_file), 'w') as file:
            file.write(rendered_content)

    logger.info(f"Rendered {len(templates)} templates to {target_path}")

def make_codeql_database(logger, repo_url, ddb):    
    logger.info(f"Cloning repository: {repo_url}")
    
    try:
        with create_tmp_cache() as clone_dir:
            clone_repo(repo_url, clone_dir)
            include = get_include(ddb)
            logger.info(f"Includes: {include}")
            remaining, deleted = clean_repo(clone_dir, include)
            logger.warn(f"Removed {deleted} files... {remaining} remaining.")
            if remaining == 0:
                logger.error(f"An unexpected error occurred... no files are left in the {repo_url}")

            db_dir = create_tmp_cache()
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

def run_query(logger, repo_url, db_dir, sig, class_name, stats_query, log_query):
    with create_tmp_cache() as query_dir:
        method_name = extract_method_name(sig)
        render_templates(logger, query_dir, method_name=method_name, class_name=class_name)
        logger.info(f"Running CodeQL queries for repository: {repo_url}, method: {sig}, class: {class_name}")
        
        try:
            stats_output = run_codeql_query(os.path.join(query_dir, stats_query), db_dir)
            results = parse_table(stats_output.splitlines())
            min_dist = float('inf')
            min_result = None
            for result in results:
                parsed_result = parse_json_values(result)
                dist = Levenshtein.distance(sig, parsed_result['gsig'])
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
            log_functions_out = run_codeql_query(os.path.join(query_dir, log_query), db_dir)
            logger.info(f"Log functions output: {log_functions_out}")
            return None


def process_method(logger, repo_url, db_dir, query_db, method_id, method_test):
    method_cm_sig = method_test['focal_method.cm_signature'][0]
    result = DBUtils.item_exists(query_db, method_cm_sig)
    
    if result:
        logger.debug(f"Using cache for method {method_cm_sig}")
        return result
    
    method_class = method_test['focal_class.identifier'][0]
    if method_id == method_class:
        logger.warning(f"Skipping constructor {method_id}")
        return 'constructor'
    
    out = run_query(logger, repo_url, db_dir, method_cm_sig, method_class, 'count_stats.ql', 'log_functions.ql')
    DBUtils.add_item(query_db, method_cm_sig, out)
    return out


def process_testcase(logger, repo_url, db_dir, query_db, test_case):
    test_cm_sig = test_case['test_case.cm_signature']
    query_key = test_case['focal_method.cm_signature'] + '_' + test_cm_sig
    
    result = DBUtils.item_exists(query_db, query_key)
    if result:
        logger.debug(f"Using cache for test {test_cm_sig}")
        return result
    
    test_class = test_case['test_class.identifier']
    
    out = run_query(logger, repo_url, db_dir, test_cm_sig, test_class, 'count_test_stats.ql', 'log_functions.ql')
    DBUtils.add_item(query_db, query_key, out)
    return out

def process_repo(repo_url, ddf):
    handler = BufferingHandler(repo_url=repo_url)
    logger = setup_logging(repo_url)

    query_env = lmdb.open('query_cache')
    stats_env = lmdb.open('stats')
    targets = ddf[ddf['repository.url'] == repo_url]

    codeql_db = make_codeql_database(logger, repo_url, targets)

    if codeql_db is None:
        DBUtils.update_stats(stats_env, success=False, level='repo', name=repo_url)
        handler.dump_logs()
        return

    methods = targets.groupby('focal_method.identifier').agg(list)
    
    logger.info(f"Found {len(methods)} methods inside repository {repo_url}")

    succeeded = False
    
    for method_id, method_test in methods.iterrows():
        method_cm_sig = method_test['focal_method.cm_signature'][0]
        logger.info(f"Processing method {method_cm_sig}")
        method_stats = process_method(logger, repo_url, codeql_db, query_env, method_id, method_test)
        if method_stats is None:
            DBUtils.update_stats(stats_env, success=False, level='method', name=method_cm_sig)
            handler.dump_logs()
            continue

        test_cases = targets[targets['focal_method.cm_signature'] == method_cm_sig]
        for i, test_case in test_cases.iterrows():
            test_cm_sig = test_case['test_case.cm_signature']
            logger.info(f"Processing testcase {test_cm_sig}")
            testcase_stats = process_testcase(logger, repo_url, codeql_db, query_env, test_case)
            if testcase_stats is None:
                handler.dump_logs()
            DBUtils.update_stats(stats_env, success=testcase_stats is not None, level='testcase', name=test_cm_sig)
            
        DBUtils.update_stats(stats_env, success=True, level='method', name=method_cm_sig)
    DBUtils.update_stats(stats_env, success=True, level='repo', name=repo_url)
    logger.warning("Cleaning up remaining files...")
    shutil.rmtree(codeql_db)

def main():
    pbar_main = open('progress.out', 'w')
    
    def process_repo_task(args):
        repo_url, ddf = args
        result = process_repo(repo_url, ddf)
        return True
        
    task_args = [(repo_url, ddf) for i, repo_url in enumerate(repositories)]

    num_processes = 1

    print("Computing results in parallel...")


    with multiprocessing.Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.imap(process_repo_task, task_args), total=len(task_args), file=pbar_main, desc="Processing repo results", unit="repo"))
        
    
if __name__ == "__main__":
    main()