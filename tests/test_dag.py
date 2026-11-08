"""Check the actual Airflow DAG configuration in the pinned container."""
from datetime import timedelta
import unittest

from airflow.models import DagBag


class DagTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bag = DagBag(dag_folder="/opt/airflow/dags")

    def test_imports_and_dependencies(self):
        self.assertFalse(self.bag.import_errors, self.bag.import_errors)
        dag = self.bag.get_dag("olist_training")
        self.assertIsNotNone(dag)
        self.assertEqual(set(dag.task_ids), {"extract", "validate", "prepare", "train"})
        for task_id, downstream in (("extract", {"validate"}), ("validate", {"prepare"}),
                                    ("prepare", {"train"}), ("train", set())):
            self.assertEqual(dag.get_task(task_id).downstream_task_ids, downstream)

    def test_schedule_retry_and_timeout_contract(self):
        dag = self.bag.get_dag("olist_training")
        self.assertEqual(dag.schedule, "0 3 * * 1")
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.get_task("extract").retries, 2)
        self.assertEqual(dag.get_task("extract").retry_delay, timedelta(seconds=30))
        self.assertEqual(dag.get_task("validate").retries, 0)
        for name in ("validate", "prepare", "train"):
            self.assertEqual(dag.get_task(name).trigger_rule, "all_success")
        for task in dag.tasks:
            self.assertGreater(task.execution_timeout.total_seconds(), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
