"""Contract tests; run inside the supplied Airflow image with unittest."""
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from olist_pipeline import REQUIRED_COLUMNS, extract, prepare, train, validate

DATASET = Path(os.environ.get("OLIST_DATASET", "/opt/olist/data/olist_orders.csv.gz"))


class ValidationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "snapshot.csv"
        self.frame = pd.read_csv(DATASET).head(10)
        self.frame.to_csv(self.path, index=False)

    def invalid(self, frame, pattern=None):
        frame.to_csv(self.path, index=False)
        with self.assertRaisesRegex(ValueError, pattern or ".+"):
            validate(str(self.path))

    def test_valid_is_read_only(self):
        before = self.path.read_bytes()
        original_open = io.open
        def read_only_open(file, mode="r", *args, **kwargs):
            if any(flag in mode for flag in "wax+"):
                raise AssertionError("validate attempted a write")
            return original_open(file, mode, *args, **kwargs)
        with patch("io.open", side_effect=read_only_open), \
             patch("builtins.open", side_effect=read_only_open), \
             patch("os.mkdir", side_effect=AssertionError("mkdir")), \
             patch("os.chdir", side_effect=AssertionError("chdir")), \
             patch("socket.socket", side_effect=AssertionError("network")), \
             patch("subprocess.Popen", side_effect=AssertionError("process")):
            self.assertEqual(validate(str(self.path)), str(self.path))
        self.assertEqual(before, self.path.read_bytes())

    def test_missing_each_required_column(self):
        for column in REQUIRED_COLUMNS:
            with self.subTest(column=column):
                self.invalid(self.frame.drop(columns=column), "missing columns")

    def test_empty(self):
        self.invalid(self.frame.iloc[:0], "at least one row")

    def test_duplicate_order(self):
        self.invalid(pd.concat([self.frame, self.frame.iloc[:1]]), "unique")

    def test_missing_order(self):
        frame = self.frame.copy()
        frame.loc[0, "order_id"] = None
        self.invalid(frame, "order_id")

    def test_bad_numeric_features(self):
        for column in ("price", "freight", "weight", "n_items", "installments", "est_days"):
            for value in (None, float("inf"), float("-inf"), "not-a-number"):
                with self.subTest(column=column, value=value):
                    frame = self.frame.astype({column: object})
                    frame.loc[0, column] = value
                    self.invalid(frame, column)

    def test_bad_labels(self):
        for value in (None, 2, -1, 0.5, "bad", float("inf")):
            with self.subTest(value=value):
                frame = self.frame.astype({"late": object})
                frame.loc[0, "late"] = value
                self.invalid(frame, "late")

    def test_bad_dates(self):
        for value in (None, "not-a-date"):
            frame = self.frame.copy()
            frame.loc[0, "order_purchase_timestamp"] = value
            self.invalid(frame)

    def test_duplicate_header(self):
        text = self.path.read_text()
        self.path.write_text(text.replace("order_id,", "price,", 1))
        with self.assertRaisesRegex(ValueError, "unique columns"):
            validate(str(self.path))


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "snapshot.csv"
        with gzip.open(DATASET, "rb") as source:
            self.path.write_bytes(source.read())
        self.environment = patch.dict(os.environ, {"OLIST_CUTOFF": "2017-07-31"})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_original_hash_split_and_idempotency(self):
        original = self.path.read_bytes()
        first = prepare(validate(str(self.path)))
        second = prepare(str(self.path))
        self.assertEqual(first, second)
        json.dumps(first)  # Must fit the XCom JSON contract.
        self.assertEqual(first["snapshot_sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(self.path.read_bytes(), original)
        self.assertEqual((first["train_rows"], first["validation_rows"]), (17928, 3735))
        a, b = pd.read_csv(first["train_path"]), pd.read_csv(first["validation_path"])
        self.assertTrue((pd.to_datetime(a.order_purchase_timestamp) < pd.Timestamp("2017-07-31")).all())
        self.assertTrue((pd.to_datetime(b.order_purchase_timestamp) >= pd.Timestamp("2017-07-31")).all())
        self.assertFalse(set(a.order_id) & set(b.order_id))
        self.assertEqual(set(a.late), {0, 1})
        self.assertEqual(set(b.late), {0, 1})

    def test_changed_files_rejected_before_training(self):
        for name in ("snapshot", "train", "validation"):
            with self.subTest(file=name):
                prepared = prepare(str(self.path))
                target = Path(prepared[f"{name}_path"])
                original = target.read_bytes()
                target.write_bytes(original + b"\n")
                try:
                    with self.assertRaisesRegex(ValueError, "has changed"):
                        train(prepared, dag_run_id="manual__integrity_test")
                finally:
                    target.write_bytes(original)


@unittest.skipUnless(os.environ.get("DATABASE_URL"), "Requires PostgreSQL via Compose")
class ExtractionTests(unittest.TestCase):
    def test_repeated_extraction_same_rows_and_hash(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"OLIST_WORKDIR": directory}):
            first = extract("manual__test/../path")
            original = Path(first).read_bytes()
            self.assertEqual(extract("manual__test/../path"), first)
            self.assertEqual(Path(first).read_bytes(), original)
            second = extract("manual__another_test")
            self.assertNotEqual(first, second)
            self.assertEqual(Path(second).read_bytes(), original)
            self.assertTrue(Path(first).is_absolute())
            self.assertTrue(Path(first).is_relative_to(directory))
            self.assertEqual(validate(first), first)
            self.assertEqual(len(pd.read_csv(first)), 21663)


if __name__ == "__main__":
    unittest.main(verbosity=2)
