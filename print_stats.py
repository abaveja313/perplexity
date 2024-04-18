import lmdb
from dataset_utils import DBUtils

stats_env = lmdb.open('/matx/u/abaveja/stats_matx1')

print("Completed Repo:", DBUtils.item_exists(stats_env, 'completed_repo'))
print("Completed Method:", DBUtils.item_exists(stats_env, 'completed_method'))
print("Completed Testcase:", DBUtils.item_exists(stats_env, 'completed_testcase'))

print("Failed Repo:", DBUtils.item_exists(stats_env, 'failed_repo_items'))
print("Failed Repo:", DBUtils.item_exists(stats_env, 'failed_repo'))
print("Failed Method:", DBUtils.item_exists(stats_env, 'failed_method_items'))
print("Failed Method:", DBUtils.item_exists(stats_env, 'failed_method'))
print("Failed Testcase:", DBUtils.item_exists(stats_env, 'failed_testcase_items'))
print("Failed Testcase:", DBUtils.item_exists(stats_env, 'failed_testcase'))

