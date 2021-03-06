from pyflink.table import *
from pyflink.dataset import *

# environment configuration
env = ExecutionEnvironment.get_execution_environment()
t_env = TableEnvironment.create(env, TableConfig())

# register Orders table and Result table sink in table environment
# ...

# specify table program
orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

orders.group_by("a").select("a, b.count as cnt").insert_into("result")

t_env.execute("python_job")