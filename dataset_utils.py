import os
import re
import subprocess
import traceback
import logging
from tqdm import tqdm
from datetime import datetime, timezone, timedelta
import time
import pickle
from jinja2 import Environment, FileSystemLoader
import pandas as pd
import numpy as np
import glob
import json
import fcntl
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import git

codeql_binary = "/scr/abaveja/codeql/codeql"

class NoProgressMadeException(Exception):
    pass

def no_progress_handler(signum, frame):
    raise NoProgressMadeException("No progress has been made... terminating task")

def convert_unix_to_pst(unix_timestamp):
    unix_timestamp = int(unix_timestamp)
    utc_time = datetime.fromtimestamp(unix_timestamp, timezone.utc)
    pst_time = utc_time.astimezone(timezone(timedelta(hours=-7))) 
    pst_timestamp = pst_time.strftime("%Y-%m-%d %H:%M:%S")
    return pst_timestamp

def setup_logging(repo_url, attempt):
    repo = repo_url.split(".com", 1)[1].strip("/").replace("/", "_").lower()
    logger = logging.getLogger(f"{repo}_{attempt}")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(f"%(asctime)s - %(levelname)s - {repo[:20]}({attempt}) - %(message)s")

    file_handler = logging.FileHandler(f"/scr/abaveja/logs/{repo}.log")
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

        files = glob.glob(
            os.path.join(root, "**", "*.json"), recursive=True
        )  # Search for nested files
        print(f"Found {len(files)} files to process")
        cpus = int(os.environ["SLURM_CPUS_ON_NODE"])
        with ProcessPoolExecutor(
            max_workers=cpus
        ) as executor:  # Adjust max_workers as needed
            total_files = len(files)
            num_batches = (total_files + batch_size - 1) // batch_size
            with tqdm(total=num_batches, desc=f"Processing...", unit="batches") as pbar:
                for i in range(0, total_files, batch_size):
                    batch_files = files[i : i + batch_size]
                    batch_futures = [
                        executor.submit(process_file, f) for f in batch_files
                    ]

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


def clone_repo(logger, repo_url, clone_dir):
    ssh_url = repo_url.split(".com", 1)[1]
    repo = git.Repo.clone_from(f"git@github.com:{ssh_url}", clone_dir)
    
    for commit in repo.iter_commits():
        commit_date = datetime.fromtimestamp(commit.committed_date)
        if commit_date < datetime(2021, 5, 18):
            # Check out the commit
            repo.git.checkout(commit.hexsha)
            logger.info(f"Checked out commit {commit.hexsha} from {commit_date}")
            return
        
    logger.warning("No commit found before specified date")


def create_codeql_database(repo_path, temp_dir):
    subprocess.run(
        [
            codeql_binary,
            "database",
            "create",
            temp_dir,
            "--build-mode=none",
            "--threads=25",
            "--language=java",
            "--source-root",
            repo_path,
        ],
        check=True,
        capture_output=True
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
            "30",
            "--xterm-progress=no",
            "--no-release-compatibility",
            "--no-local-checking",
            "--no-metadata-verification",
        ],
        text=True,
        check=True,
        capture_output=True,
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
                my_list = set()
            else:
                my_list = set(pickle.loads(raw_data))

            my_list.add(item)
            txn.put(key.encode(), pickle.dumps(my_list))

    @staticmethod
    def register_work(repo_url, last_updated):
        if last_updated and repo_url:
            last_updated[repo_url] = time.time()

    @staticmethod
    def update_stats(
        db, success=True, level="repo", name=None, last_updated=None, repo_url=None
    ):
        DBUtils.register_work(repo_url=repo_url, last_updated=last_updated)

        prefix = "completed" if success else "failed"

        DBUtils.increment_value(db, f"{prefix}_{level}")
        if name is not None:
            DBUtils.add_to_list(db, f"{prefix}_{level}_items", name)

        DBUtils.sync_stats(db, "/matx/u/abaveja/stats.json")

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

    @staticmethod
    def sync_data(data, output_file):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                json.dump(data, f, indent=2)
                f.write("\n")
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    @staticmethod
    def sync_stats(env, output_file):
        data = {
            "completed_repo": DBUtils.item_exists(env, "completed_repo"),
            "completed_method": DBUtils.item_exists(env, "completed_method"),
            "completed_testcase": DBUtils.item_exists(env, "completed_testcase"),
            "failed_repo": DBUtils.item_exists(env, "failed_repo"),
            "failed_method": DBUtils.item_exists(env, "failed_method"),
            "failed_testcase": DBUtils.item_exists(env, "failed_testcase"),
        }

        DBUtils.sync_data(data, output_file)
