import os
import re
import subprocess
import tempfile
import traceback
import logging
from tqdm.notebook import tqdm
import shelve
import pprint
import pickle
import shutil
from jinja2 import Environment, FileSystemLoader

codeql_binary = "/matx/u/abaveja/codeql/codeql"

def setup_logging(repo_url):
    r = repo_url.split('.com', 1)[1].strip('/').replace('/', '_').lower()
    logger = logging.getLogger(f"{r}")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(f"mlogs/{r}.log")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def clean_repo(directory, keeplist):
    """
    Further adjusted version to return the remaining files and the number of files deleted.
    """

    directory = os.path.abspath(directory)
    absolute_keeplist = [os.path.join(directory, item) if not os.path.isabs(item) else item for item in keeplist]
    relative_keeplist = [os.path.relpath(item, start=directory) if os.path.isabs(item) else item for item in keeplist]

    deleted_files_count = 0

    def is_in_keeplist(file_path):
        absolute_path = os.path.abspath(file_path)
        relative_path = os.path.relpath(file_path, start=directory)
        return any(absolute_path == kept_item or absolute_path.startswith(os.path.join(kept_item, '')) for kept_item in absolute_keeplist) or \
               any(relative_path == kept_item or relative_path.startswith(os.path.join(kept_item, '')) for kept_item in relative_keeplist)

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

    return remaining_files, deleted_files_count

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
        f"git clone git@github.com:{ssh_url} {clone_dir}",
        shell=True,
        capture_output=True,
        check=True,
    )


def create_codeql_database(repo_path, temp_dir):
    subprocess.run(
        [
            codeql_binary,
            "database",
            "create",
            temp_dir,
            "--threads", "20",
            "--language=java",
            "--build-mode=none",
            "--source-root",
            repo_path,
            "--no-run-unnecessary-builds",
            "--no-tracing",
        ],
        capture_output=True,
        check=True,
    )


def run_codeql_query(query_path, temp_dir):
    result = subprocess.run(
        [
            codeql_binary,
            "query",
            "run",
            query_path,
            "--database",
            temp_dir,
            "--threads", "40",
            "--save-cache",
            "--xterm-progress=no",
            "--no-release-compatibility",
            "--no-local-checking",
            "--no-metadata-verification"
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
            with open(os.path.join(query_dir, file), 'r') as f:
                logger.error(f.read())

def extract_result(text):
    match = re.search(r'\|\s*(\d+)\s*\|', text)
    if match:
        return int(match.group(1))


def get_params(cm_sig):
    match = re.search(r"(\(.*\))", cm_sig)
    if match:
        return match.group(1)


def parse_table(lines):
    header_line_idx = None
    for i, line in enumerate(lines):
        if re.match(r'\+\-+\+', line):
            header_line_idx = i - 1
            break
            
    if header_line_idx is None:
        return []
    
    col_names = [name.strip() for name in lines[header_line_idx].split('|')[1:-1]]
    rows = []
    for line in lines[header_line_idx + 2:]:
        if line.startswith('+'):
            break
        row = [cell.strip() for cell in line.split('|')[1:-1]]
        if row:
            rows.append(dict(zip(col_names, row)))
            
    return rows


def parse_json_values(json_obj):
    for key, value in json_obj.items():
        if value.lower() == 'true':
            json_obj[key] = True
        elif value.lower() == 'false':
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
    def update_stats(db, success=True, level='repo', name=None):
        prefix = 'completed' if success else 'failed'
        
        DBUtils.increment_value(db, f'{prefix}_{level}')
        if name is not None:
            DBUtils.add_to_list(db, f'{prefix}_{level}_items', name)

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
                
        

