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
import pandas as pd
import numpy as np
import json
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

codeql_binary = "/matx/u/abaveja/codeql/codeql"

class BufferingHandler(logging.Handler):
    def __init__(self, repo_url):
        super().__init__()
        self.buffer = []
        self.repo = repo_url.split('.com', 1)[1].strip('/').replace('/', '_').lower()

    def emit(self, record):
        self.buffer.append(record)

    def flush(self):
        pass  # Override to do nothing.

    def dump_logs(self):
        with open(os.path.join('mlogs', self.repo), 'a') as f:
            for record in self.buffer:
                msg = self.format(record)
                f.write(msg + '\n')
        self.buffer = []  # Clear the buffer after dumping logs


def get_cache_creator(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)
    
    create_tmp_cache = lambda: tempfile.TemporaryDirectory(path)
    return create_tmp_cache    

def setup_logging(repo_url, handler):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def get_templates(query_dir='queries/'):
    env = Environment(loader=FileSystemLoader(query_dir))

    template_files = [f for f in os.listdir(query_dir) if f.endswith('.ql')]

    templates = {template_file: env.get_template(template_file) for template_file in template_files}
    return templates

def get_or_create_df(path='df.parquet.gzip'):
    data = []
    if not os.path.exists(path):
        root = '/matx/u/abaveja/methods2test/dataset/'
        splits = ['train', 'eval', 'test']
        batch_size = 2500

        with ProcessPoolExecutor() as executor:
            for split in splits:
                path = Path(os.path.join(root, split))
                files = list(path.rglob('*'))
                total_files = len(files)
                
                with tqdm(total=total_files, desc=f"Processing {split}", unit='files') as split_pbar:
                    for i in range(0, len(files), batch_size):
                        batch_files = files[i:i+batch_size]
                        futures = [executor.submit(process_file, f, split) for f in batch_files if f.is_file()]
                        
                        batch_data = []
                        for future in as_completed(futures):
                            row = future.result()
                            if row is not None:
                                batch_data.append(row)
                        
                        data.extend(batch_data)
                        split_pbar.update(len(batch_files))

        df = pd.DataFrame(data)
        df.to_parquet(path, compression='gzip')
    else:
        df = pd.read_parquet(path)

    return df



def extract_columns(fpath: Path) -> dict:
    """
    Extracts specific columns from a JSON file.

    Args:
        fpath (Path): The path to the JSON file.

    Returns:
        dict: A dictionary containing the extracted data.
    """
    with open(fpath, 'r') as f:
        data = json.load(f)
        extracted_data = {
            'test_case.identifier': data['test_case']['identifier'],
            'test_case.invocations': data['test_case']['invocations'],
            'test_case.body': data['test_case']['body'],
            'test_case.modifiers': data.get('test_case', {}).get('modifiers', None),
            'test_case.cm_signature': data['test_case']['class_method_signature'],
            'test_class.identifier': data['test_class']['identifier'],
            'test_class.file': data['test_class']['file'],
            'focal_class.identifier': data['focal_class']['identifier'],
            'focal_class.file': data['focal_class']['file'],
            'focal_method.identifier': data['focal_method']['identifier'],
            'focal_method.modifiers': data['focal_method']['modifiers'],
            'focal_method.return': data['focal_method']['return'],
            'focal_method.body': data['focal_method']['body'],
            'focal_method.cm_signature': data['focal_method']['class_method_signature'],
            'repository.repo_id': data.get('repository', {}).get('repo_id'),
            'repository.size': data.get('repository', {}).get('size'),
            'repository.url': data.get('repository', {}).get('url'),
            'repository.stargazer_count': data.get('repository', {}).get('stargazer_count')
        }
        return extracted_data

def process_file(f: Path, split: str) -> dict:
    """
    Processes a file by extracting its columns and adding the split information.

    Args:
        f (Path): The path to the file.
        split (str): The split identifier.

    Returns:
        dict: A dictionary containing the processed data.
    """
    try:
        row = extract_columns(f.absolute())
        row['split'] = split
        return row
    except Exception as e:
        print(f"Error processing file: {f}")
        traceback.print_exc()
        return None

def get_partition(ddf: pd.DataFrame, num_partitions: int, partition_index: int) -> pd.DataFrame:
    """
    Partitions a DataFrame based on unique repository URLs.

    Args:
        ddf (pd.DataFrame): The DataFrame to partition.
        num_partitions (int): The total number of partitions.
        partition_index (int): The index of the current partition.

    Returns:
        pd.DataFrame: The partitioned DataFrame.
    """
    unique_urls = ddf['repository.url'].unique()
    sorted_urls = np.sort(unique_urls)
    partition_size = len(sorted_urls) // num_partitions
    start_index = partition_index * partition_size
    end_index = (partition_index + 1) * partition_size
    if partition_index == num_partitions - 1:
        end_index = len(sorted_urls)
    
    partition_urls = sorted_urls[start_index:end_index]
    partition_df = ddf[ddf['repository.url'].isin(partition_urls)]
    return partition_df

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
                
        

