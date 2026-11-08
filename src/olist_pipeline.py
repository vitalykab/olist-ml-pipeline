"""Restartable PostgreSQL → CSV → CPU model → MLflow stages.

Only paths and JSON-serializable metadata cross Airflow task boundaries.
Imports and validate() have no filesystem/network write side effects.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
from pathlib import Path
import tempfile

NUMERIC_COLUMNS = ("price", "freight", "weight", "n_items", "installments", "est_days")
CATEGORICAL_COLUMNS = ("customer_state", "seller_state", "category", "payment_type")
REQUIRED_COLUMNS = ("order_id", "order_purchase_timestamp", *NUMERIC_COLUMNS,
                    *CATEGORICAL_COLUMNS, "late")


def _snapshot_frame(content):
    """Parse without silently accepting duplicate CSV column names."""
    import pandas as pd

    header = next(csv.reader(io.StringIO(content.decode("utf-8"))), [])
    if not header or len(header) != len(set(header)):
        raise ValueError("Snapshot must have a nonempty header with unique columns")
    missing = sorted(set(REQUIRED_COLUMNS) - set(header))
    if missing:
        raise ValueError(f"Snapshot is missing columns: {', '.join(missing)}")
    return pd.read_csv(io.BytesIO(content), encoding="utf-8", dtype={"order_id": "string"})


def _atomic_csv(frame, destination):
    """Never expose a partially written CSV to a subsequent task/retry."""
    destination = Path(destination)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="",
                                         dir=destination.parent, suffix=".tmp", delete=False) as output:
            temporary = Path(output.name)
            frame.to_csv(output, index=False, lineterminator="\n")
        temporary.replace(destination)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def extract(run_key):
    """Read public.orders and return an absolute path to a plain UTF-8 CSV."""
    import psycopg

    if not isinstance(run_key, str) or not run_key.strip():
        raise ValueError("run_key must be a nonempty Airflow run ID")
    run_directory = Path(os.environ["OLIST_WORKDIR"]).expanduser().resolve() / hashlib.sha256(run_key.encode()).hexdigest()
    run_directory.mkdir(parents=True, exist_ok=True)
    destination = run_directory / "snapshot.csv"
    database_url = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    query = (f"COPY (SELECT {', '.join(REQUIRED_COLUMNS)} FROM public.orders "
             "ORDER BY order_purchase_timestamp, order_id) "
             "TO STDOUT WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')")
    temporary = None
    try:
        with psycopg.connect(database_url, connect_timeout=15) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET TIME ZONE 'UTC'")
                cursor.execute("SET DateStyle TO ISO, YMD")
                with tempfile.NamedTemporaryFile(mode="wb", dir=run_directory,
                                                 suffix=".tmp", delete=False) as output:
                    temporary = Path(output.name)
                    with cursor.copy(query) as stream:
                        for block in stream:
                            output.write(block)
        temporary.replace(destination)
    except psycopg.Error as exc:
        # A DSN may contain a password; do not include connection details in logs.
        raise RuntimeError(f"PostgreSQL extraction failed (SQLSTATE {exc.sqlstate or 'connection error'})") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return str(destination)


def validate(snapshot):
    """Read-only validation: no writes, mkdir/chdir, processes, network or DB."""
    import numpy as np
    import pandas as pd

    path = Path(snapshot)
    if path.suffix != ".csv":
        raise ValueError("Snapshot must be a plain UTF-8 .csv file")
    frame = _snapshot_frame(path.read_bytes())
    if frame.empty:
        raise ValueError("Snapshot must contain at least one row")
    ids = frame["order_id"]
    if ids.isna().any() or ids.str.strip().eq("").any():
        raise ValueError("order_id must not be missing or empty")
    if ids.duplicated().any():
        raise ValueError("order_id must be unique")
    for name in (*NUMERIC_COLUMNS, "late"):
        try:
            values = pd.to_numeric(frame[name], errors="raise").to_numpy(dtype=np.float64)
        except (ValueError, TypeError) as exc:
            raise ValueError(f"{name} must contain numeric values") from exc
        if not np.isfinite(values).all():
            raise ValueError(f"{name} must contain finite values (no missing/NaN/Inf)")
        if name == "late" and not np.isin(values, [0, 1]).all():
            raise ValueError("late must contain only 0 or 1")
    dates = pd.to_datetime(frame["order_purchase_timestamp"], format="ISO8601", errors="raise", utc=True)
    if dates.isna().any():
        raise ValueError("order_purchase_timestamp must not be missing")
    for name in CATEGORICAL_COLUMNS:
        if not frame[name].dropna().map(lambda value: isinstance(value, str)).all():
            raise ValueError(f"{name} must contain strings or missing values")
    return str(path)


def prepare(snapshot):
    """Persist the supplied chronological split and original snapshot metadata."""
    from olist_model import split_by_time

    path = Path(snapshot).resolve()
    content = path.read_bytes()
    cutoff = os.environ.get("OLIST_CUTOFF", "2017-07-31")
    train_frame, validation_frame = split_by_time(_snapshot_frame(content), cutoff=cutoff)
    train_path = path.parent / "train.csv"
    validation_path = path.parent / "validation.csv"
    _atomic_csv(train_frame, train_path)
    _atomic_csv(validation_frame, validation_path)
    return {
        "snapshot_path": str(path),
        "snapshot_sha256": hashlib.sha256(content).hexdigest(),
        "train_path": str(train_path),
        "validation_path": str(validation_path),
        "train_sha256": hashlib.sha256(train_path.read_bytes()).hexdigest(),
        "validation_sha256": hashlib.sha256(validation_path.read_bytes()).hexdigest(),
        "cutoff": cutoff,
        "train_rows": len(train_frame),
        "validation_rows": len(validation_frame),
    }


def train(prepared, *, dag_run_id, epochs=None, seed=None):
    """Train the supplied predictor, upload the complete model and return run_id."""
    import mlflow
    from olist_model import fit_model, save_predictor

    if not isinstance(dag_run_id, str) or not dag_run_id:
        raise ValueError("dag_run_id must be the actual Airflow context run_id")
    frames = {}
    for name in ("snapshot", "train", "validation"):
        content = Path(prepared[f"{name}_path"]).read_bytes()
        if hashlib.sha256(content).hexdigest() != prepared[f"{name}_sha256"]:
            raise ValueError(f"{name} has changed since prepare; refusing to train on different data")
        if name != "snapshot":
            frames[name] = _snapshot_frame(content)
            if len(frames[name]) != prepared[f"{name}_rows"]:
                raise ValueError(f"{name} row count differs from prepare metadata")
    epochs = int(os.environ.get("OLIST_EPOCHS", "5")) if epochs is None else epochs
    seed = int(os.environ.get("OLIST_SEED", "42")) if seed is None else seed
    lr = float(os.environ.get("OLIST_LR", "0.003"))
    mlflow.set_tracking_uri(os.environ["MLFLOW_TRACKING_URI"])
    mlflow.set_experiment(os.environ.get("MLFLOW_EXPERIMENT_NAME", "olist_training"))
    with mlflow.start_run(run_name=dag_run_id, tags={"dag_run_id": dag_run_id}) as run:
        mlflow.log_params({
            "epochs": epochs, "seed": seed, "lr": lr,
            "cutoff": prepared["cutoff"], "train_rows": prepared["train_rows"],
            "validation_rows": prepared["validation_rows"],
            "snapshot_sha256": prepared["snapshot_sha256"],
            "snapshot_path": prepared["snapshot_path"],
        })
        mlflow.log_dict(prepared, "dataset.json")
        predictor, history = fit_model(frames["train"], frames["validation"],
                                       epochs=epochs, seed=seed, lr=lr)
        for row in history:
            mlflow.log_metrics({"train_loss": row["train_loss"],
                                "validation_pr_auc": row["validation_pr_auc"]}, step=row["epoch"])
        model_directory = Path(prepared["snapshot_path"]).parent / "model"
        save_predictor(predictor, model_directory)
        mlflow.log_artifacts(str(model_directory), artifact_path="model")
        return run.info.run_id
