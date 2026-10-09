import os
import tempfile

# Isolate tests from any local Airflow install: own home, no examples, no metadata DB needed.
os.environ["AIRFLOW_HOME"] = tempfile.mkdtemp(prefix="airflow-test-")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
