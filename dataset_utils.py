import os
import re
import subprocess
import tempfile
import traceback
import logging
from tqdm import tqdm
import shelve
import pprint
import pickle
import shutil
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import numpy as np
import glob
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

codeql_binary = "/scr/abaveja/codeql/codeql"

def get_cache_creator(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

    create_tmp_cache = lambda: tempfile.TemporaryDirectory(dir=path)
    return create_tmp_cache

def setup_logging(repo_url):
    repo = repo_url.split(".com", 1)[1].strip("/").replace("/", "_").lower()
    logger = logging.getLogger(repo)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    file_handler = logging.FileHandler(f'/scr/abaveja/logs/{repo}.log')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Return the configured logger
    return logger

def get_templates(query_dir="queries/"):
    env = Environment(loader=FileSystemLoader(query_dir))

    template_files = [f for f in os.listdir(query_dir) if f.endswith(".ql")]

    templates = {
        template_file: env.get_template(template_file)
        for template_file in template_files
    }
    return templates


def process_file(f: Path) -> dict:
    """
    Processes a file by extracting its columns and adding the split information.

    Args:
        f (Path): The path to the file.
        split (str): The split identifier.

    Returns:
        dict: A dictionary containing the processed data.
    """
    try:
        row = extract_columns(f)
        return row
    except Exception as e:
        print(f"Error processing file: {f}")
        traceback.print_exc()
        return None


def get_or_create_df(df_path="df.feather"):
    if not os.path.exists(df_path):
        print("Creating DF...")
        root = "/scr/abaveja/methods2test/dataset"
        batch_size = 2000
        data = []  # Initialize data list outside the split loop

        files = glob.glob(os.path.join(root, "**", "*.json"), recursive=True)  # Search for nested files
        print(f"Found {len(files)} files to process")
        cpus = int(os.environ['SLURM_CPUS_ON_NODE'])
        with ProcessPoolExecutor(max_workers=cpus) as executor:  # Adjust max_workers as needed
            total_files = len(files)
            num_batches = (total_files + batch_size - 1) // batch_size
            with tqdm(total=num_batches, desc=f"Processing...", unit="batches") as pbar:
                for i in range(0, total_files, batch_size):
                    batch_files = files[i:i + batch_size]
                    batch_futures = [executor.submit(process_file, f) for f in batch_files]
                    
                    for future in as_completed(batch_futures):
                        row = future.result()
                        if row:
                            data.append(row)
                    
                    pbar.update(1)  # Update progress bar for each completed batch

        df = pd.DataFrame.from_records(data)  # Create DataFrame from list of rows
        df.to_feather(df_path)  # Save DataFrame as Feather file
    else:
        df = pd.read_feather(df_path)  # Read DataFrame from Feather file
    return df

def extract_columns(fpath: Path) -> dict:
    """
    Extracts specific columns from a JSON file.

    Args:
        fpath (Path): The path to the JSON file.

    Returns:
        dict: A dictionary containing the extracted data.
    """
    with open(fpath, "r") as f:
        data = json.load(f)
        extracted_data = {
            "test_case.identifier": data["test_case"]["identifier"],
            "test_case.invocations": data["test_case"]["invocations"],
            "test_case.body": data["test_case"]["body"],
            "test_case.modifiers": data.get("test_case", {}).get("modifiers", None),
            "test_case.cm_signature": data["test_case"]["class_method_signature"],
            "test_class.identifier": data["test_class"]["identifier"],
            "test_class.file": data["test_class"]["file"],
            "focal_class.identifier": data["focal_class"]["identifier"],
            "focal_class.file": data["focal_class"]["file"],
            "focal_method.identifier": data["focal_method"]["identifier"],
            "focal_method.modifiers": data["focal_method"]["modifiers"],
            "focal_method.return": data["focal_method"]["return"],
            "focal_method.body": data["focal_method"]["body"],
            "focal_method.cm_signature": data["focal_method"]["class_method_signature"],
            "repository.repo_id": data.get("repository", {}).get("repo_id"),
            "repository.size": data.get("repository", {}).get("size"),
            "repository.url": data.get("repository", {}).get("url"),
            "repository.stargazer_count": data.get("repository", {}).get(
                "stargazer_count"
            ),
        }
        return extracted_data


def get_partition(
    ddf: pd.DataFrame, num_partitions: int, partition_index: int
) -> pd.DataFrame:
    """
    Partitions a DataFrame based on unique repository URLs.

    Args:
        ddf (pd.DataFrame): The DataFrame to partition.
        num_partitions (int): The total number of partitions.
        partition_index (int): The index of the current partition.

    Returns:
        pd.DataFrame: The partitioned DataFrame.
    """
    unique_urls = ddf["repository.url"].unique()
    sorted_urls = np.sort(unique_urls)
    partition_size = len(sorted_urls) // num_partitions
    start_index = partition_index * partition_size
    end_index = (partition_index + 1) * partition_size
    if partition_index == num_partitions - 1:
        end_index = len(sorted_urls)

    partition_urls = sorted_urls[start_index:end_index]
    partition_df = ddf[ddf["repository.url"].isin(partition_urls)]
    return partition_df


def clean_repo(directory, keeplist):
    """
    Further adjusted version to return the remaining files and the number of files deleted.
    """

    directory = os.path.abspath(directory)
    absolute_keeplist = [
        os.path.join(directory, item) if not os.path.isabs(item) else item
        for item in keeplist
    ]
    relative_keeplist = [
        os.path.relpath(item, start=directory) if os.path.isabs(item) else item
        for item in keeplist
    ]

    deleted_files_count = 0

    def is_in_keeplist(file_path):
        absolute_path = os.path.abspath(file_path)
        relative_path = os.path.relpath(file_path, start=directory)
        return any(
            absolute_path == kept_item
            or absolute_path.startswith(os.path.join(kept_item, ""))
            for kept_item in absolute_keeplist
        ) or any(
            relative_path == kept_item
            or relative_path.startswith(os.path.join(kept_item, ""))
            for kept_item in relative_keeplist
        )

    for root, dirs, files in os.walk(directory, topdown=False):
        for name in files:
            file_path = os.path.join(root, name)
            if not is_in_keeplist(file_path):
                os.remove(file_path)
                deleted_files_count += 1

        for name in dirs:
            dir_path = os.path.join(root, name)
            if not os.listdir(dir_path) and not is_in_keeplist(dir_path):
                os.rmdir(dir_path)

    # Collect remaining files after deletion
    remaining_files = []
    for root, dirs, files in os.walk(directory, topdown=True):
        for name in files:
            remaining_files.append(os.path.join(root, name))

    return len(remaining_files), deleted_files_count


def is_valid_java_repo(directory):
    if not os.path.exists(directory):
        logger.error(f"Directory '{directory}' does not exist.")
        return False

    items = os.listdir(directory)

    if "gradle" in items or "gradlew" in items:
        return True

    if "pom.xml" in items:
        return True

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".java"):
                return True

    return False


def clone_repo(repo_url, clone_dir):
    ssh_url = repo_url.split(".com", 1)[1]
    subprocess.run(
        f'git clone git@github.com:{ssh_url} {clone_dir}',
        shell=True,
        check=True,
    )


def create_codeql_database(repo_path, temp_dir):
    subprocess.run(
        [
            codeql_binary,
            "database",
            "create",
            temp_dir,
            "--build-mode=none",
            "--threads=20",
            "--language=java",
            "--source-root",
            repo_path,
        ],
        check=True,
    )

def parse_table(lines):
    header_line_idx = None
    for i, line in enumerate(lines):
        if re.match(r"\+\-+\+", line):
            header_line_idx = i - 1
            break

    if header_line_idx is None:
        return []

    col_names = [name.strip() for name in lines[header_line_idx].split("|")[1:-1]]
    rows = []
    for line in lines[header_line_idx + 2 :]:
        if line.startswith("+"):
            break
        row = [cell.strip() for cell in line.split("|")[1:-1]]
        if row:
            rows.append(dict(zip(col_names, row)))

    return rows


def run_codeql_query(query_path, temp_dir):
    result = subprocess.run(
        [
            codeql_binary,
            "query",
            "run",
            query_path,
            "--database",
            temp_dir,
            "--threads",
            "10",
            "--xterm-progress=no",
            "--no-release-compatibility",
            "--no-local-checking",
            "--no-metadata-verification",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def log_queries(logger, query_dir):
    logger.error("Query files:")
    for file in os.listdir(query_dir):
        if file.endswith(".ql"):
            logger.error(file)
            with open(os.path.join(query_dir, file), "r") as f:
                logger.error(f.read())


def extract_result(text):
    match = re.search(r"\|\s*(\d+)\s*\|", text)
    if match:
        return int(match.group(1))


def get_params(cm_sig):
    match = re.search(r"(\(.*\))", cm_sig)
    if match:
        return match.group(1)


def parse_csv_results(csv_file):
    results = []  
    with open(csv_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            results.append(row)
    return results


def parse_json_values(json_obj):
    for key, value in json_obj.items():
        if value.lower() == "true":
            json_obj[key] = True
        elif value.lower() == "false":
            json_obj[key] = False
        else:
            try:
                json_obj[key] = int(value)
            except ValueError:
                try:
                    json_obj[key] = float(value)
                except ValueError:
                    continue
    return json_obj


class DBUtils:
    @staticmethod
    def increment_value(env, key):
        with env.begin(write=True) as txn:
            raw_value = txn.get(key.encode())
            if raw_value is None:
                value = 0
            else:
                value = pickle.loads(raw_value)

            value += 1
            txn.put(key.encode(), pickle.dumps(value))

    @staticmethod
    def add_to_list(env, key, item):
        with env.begin(write=True) as txn:
            raw_data = txn.get(key.encode())
            if raw_data is None:
                my_list = []
            else:
                my_list = pickle.loads(raw_data)

            my_list.append(item)
            txn.put(key.encode(), pickle.dumps(my_list))

    @staticmethod
    def remove_from_list(env, key):
        with env.begin(write=True) as txn:
            raw_data = txn.get(key.encode())
            if raw_data is None:
                return
            else:
                my_list = pickle.loads(raw_data)

            my_list.remove(item)
            txn.put(key.encode(), pickle.dumps(my_list))

    @staticmethod
    def update_stats(db, success=True, level="repo", name=None):
        prefix = "completed" if success else "failed"

        DBUtils.increment_value(db, f"{prefix}_{level}")
        if name is not None:
            DBUtils.add_to_list(db, f"{prefix}_{level}_items", name)

    @staticmethod
    def add_item(env, key, results):
        with env.begin(write=True) as txn:
            txn.put(key.encode(), pickle.dumps(results))

    @staticmethod
    def item_exists(env, key):
        with env.begin(write=False) as txn:
            raw_value = txn.get(key.encode())
            if raw_value is not None:
                return pickle.loads(raw_value)