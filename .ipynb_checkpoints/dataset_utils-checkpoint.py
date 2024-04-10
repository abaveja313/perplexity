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


def clean_repo(logger, repo_path, keep_list):
    total_files = 0
    deleted_files = 0
    
    keep_list = [os.path.normpath(path) for path in keep_list]
    
    for root, dirs, files in os.walk(repo_path, topdown=False):
        for name in files:
            total_files += 1
            file_relative_path = os.path.relpath(os.path.join(root, name), start=repo_path)
            if file_relative_path not in keep_list:
                file_path = os.path.join(root, name)
                try:
                    os.remove(file_path)
                    deleted_files += 1
                except OSError as e:
                    logger.error(f"Error deleting file: {file_path} : {e.strerror}")
        
        for name in dirs:
            dir_relative_path = os.path.relpath(os.path.join(root, name), start=repo_path)
            dir_path = os.path.join(root, name)
            if not any(item.startswith(dir_relative_path + os.sep) for item in keep_list):
                if not os.listdir(dir_path):
                    try:
                        shutil.rmtree(dir_path)
                    except OSError as e:
                        logger.error(f"Error deleting directory: {dir_path} : {e.strerror}")
    
    remaining_files = total_files - deleted_files
    return deleted_files, total_files, remaining_files


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
                
        

