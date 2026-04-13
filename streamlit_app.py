import io
import json
import os
import shutil
import tempfile
import importlib.util
from datetime import datetime
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict, Tuple

import pandas as pd
import streamlit as st


SAMPLE_FILES = {
    "line_items": "Point of Sale Orders (pos.order) - 2026-04-12T162030.266.xlsx",
    "payments": "Point of Sale Orders (pos.order) - 2026-04-12T162041.258.xlsx",
    "metadata": "FUSION_SALES_METADATA_202604121703.csv",
    "registers": "VENDHQ_REGISTERS_202604121654.csv",
}

STATE_FILE = Path(__file__).parent / "state" / "run_state.json"
DEFAULT_PREFIX = "BULK-ALAJH"


@st.cache_resource
def get_integration_class():
    """Dynamically load the integration class from the existing script."""
    module_path = Path(__file__).parent / "Odoo-export-FBDA-template.py"
    spec = importlib.util.spec_from_file_location("fusion_template", module_path)
    if not spec or not spec.loader:
        raise ImportError("Unable to load OracleFusionIntegration from the template script.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.OracleFusionIntegration


@st.cache_resource
def get_expected_columns() -> int:
    """Return the number of AR columns so we can confirm nothing is lost."""
    temp_dir = Path(tempfile.mkdtemp(prefix="fusion_columns_"))
    integration_cls = get_integration_class()
    instance = integration_cls(base_output_dir=str(temp_dir))
    return len(instance.ar_columns)


def load_state() -> Dict[str, Any]:
    """Load persisted run state (last transaction numbers, run history)."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            pass
    return {"prefixes": {}, "runs": [], "last_prefix": DEFAULT_PREFIX}


def save_state(state: Dict[str, Any]) -> None:
    """Persist run state to disk."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2)


def get_next_sequence(state: Dict[str, Any], prefix: str) -> int:
    """Return the next starting sequence for a prefix."""
    last_used = state.get("prefixes", {}).get(prefix, 0)
    return last_used + 1


def persist_upload(upload, target_dir: Path) -> str:
    """Save an uploaded file to disk and return its path."""
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / upload.name
    with open(path, "wb") as f:
        f.write(upload.getbuffer())
    return str(path)


def get_sample_paths(base_dir: Path) -> Tuple[Dict[str, str], Tuple[str, ...]]:
    """Return sample file paths if they exist."""
    resolved = {}
    missing = []
    for key, filename in SAMPLE_FILES.items():
        candidate = base_dir / filename
        if candidate.exists():
            resolved[key] = str(candidate)
        else:
            missing.append(filename)
    return resolved, tuple(missing)


def generate_templates(input_paths: Dict[str, str], output_dir: Path, prefix: str, starting_seq: int) -> Tuple[str, Dict[str, Any]]:
    """Run the pipeline and return captured stdout plus run results."""
    output_dir.mkdir(parents=True, exist_ok=True)
    integration_cls = get_integration_class()
    integration = integration_cls(
        base_output_dir=str(output_dir),
        transaction_prefix=prefix,
        starting_sequence=starting_seq,
    )

    log_stream = io.StringIO()
    with redirect_stdout(log_stream):
        result = integration.run(
            input_paths["line_items"],
            input_paths["payments"],
            input_paths["metadata"],
            input_paths["registers"],
        )
    return log_stream.getvalue(), result


def summarize_outputs(output_dir: Path) -> Tuple[int, int]:
    """Count AR invoice rows and receipt rows for a quick health check."""
    ar_rows = 0
    receipt_rows = 0

    ar_dir = output_dir / "AR_Invoices"
    if ar_dir.exists():
        latest_ar = sorted(ar_dir.glob("AR_Invoice_Import_*.csv"), key=os.path.getmtime)
        if latest_ar:
            ar_rows = len(pd.read_csv(latest_ar[-1], encoding="utf-8-sig"))

    receipts_dir = output_dir / "Receipts"
    if receipts_dir.exists():
        for receipt_file in receipts_dir.glob("Receipt_Import_*.csv"):
            receipt_rows += len(pd.read_csv(receipt_file, encoding="utf-8-sig"))

    return ar_rows, receipt_rows


def update_state_with_run(state: Dict[str, Any], prefix: str, start_seq: int, run_result: Dict[str, Any]) -> Dict[str, Any]:
    """Record last transaction number and append run history."""
    ar_stats = run_result.get("ar_stats", {})
    last_seq = ar_stats.get("last_transaction_number", start_seq - 1)
    invoice_count = ar_stats.get("invoice_count", 0)
    total_sales_amount = ar_stats.get("total_sales_amount", 0.0)
    ar_rows = len(run_result.get("ar_df", []))
    receipt_rows = sum(len(df) for df in run_result.get("receipt_files", {}).values())

    state.setdefault("prefixes", {})[prefix] = last_seq
    state["last_prefix"] = prefix
    state.setdefault("runs", []).append(
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "prefix": prefix,
            "start_seq": start_seq,
            "end_seq": last_seq,
            "orders": invoice_count,
            "total_sales": total_sales_amount,
            "ar_rows": ar_rows,
            "receipt_rows": receipt_rows,
        }
    )
    return state


def render_validation(ar_df: pd.DataFrame, ar_stats: Dict[str, Any], expected_cols: int, ar_rows: int, receipt_rows: int):
    """Render validation checks so users can confirm nothing is missing."""
    transaction_map = ar_stats.get("transaction_number_map", {})
    validations = [
        ("AR column count matches template", len(ar_df.columns) == expected_cols),
        ("Transaction numbers assigned per payment family", bool(transaction_map)),
        ("Transaction numbers present on all AR rows", ar_df["Transaction Number"].notna().all()),
        ("AR rows generated", ar_rows > 0),
        ("Receipts generated", receipt_rows > 0),
    ]

    st.subheader("Validation checks")
    for label, ok in validations:
        if ok:
            st.success(f"✅ {label}")
        else:
            st.error(f"❌ {label}")

    if transaction_map:
        prefix = ar_stats.get("transaction_prefix", "")
        st.caption(
            "Transaction numbering used this run: "
            + ", ".join(
                f"{k}: {prefix + '-' if prefix else ''}{v:04d}"
                for k, v in transaction_map.items()
            )
        )


def render_history(state: Dict[str, Any]):
    """Show day-wise run history for audit."""
    runs = state.get("runs", [])
    if not runs:
        return
    st.subheader("Daily run history")
    df = pd.DataFrame(runs)
    df = df.sort_values(by="timestamp", ascending=False)
    df.rename(
        columns={
            "timestamp": "Timestamp",
            "prefix": "Prefix",
            "start_seq": "Start #",
            "end_seq": "End #",
            "orders": "Orders",
            "total_sales": "Total Sales",
            "ar_rows": "AR Rows",
            "receipt_rows": "Receipt Rows",
        },
        inplace=True,
    )
    st.dataframe(df, hide_index=True, use_container_width=True)


def main():
    st.set_page_config(
        page_title="Oracle Fusion Template Generator",
        page_icon=":ledger:",
        layout="wide",
    )
    st.title("Oracle Fusion Bulk Template Generator")
    st.write(
        "Upload POS exports and metadata to produce FBDA-ready AR invoices and receipts "
        "without losing a single column. Includes transaction-number tracking and run history."
    )

    state = load_state()
    expected_cols = get_expected_columns()

    config_col, data_col = st.columns([1, 2])
    with config_col:
        st.subheader("Transaction numbering")
        prefix = st.text_input(
            "Transaction prefix",
            value=state.get("last_prefix", DEFAULT_PREFIX),
            help="Example: BULK-ALAJH",
        ).strip() or DEFAULT_PREFIX
        next_seq_default = get_next_sequence(state, prefix)
        starting_seq = st.number_input(
            "Starting sequence for this run",
            min_value=1,
            value=next_seq_default,
            step=1,
            help="This run will assign numbers from this sequence onward and persist the last used.",
        )
        st.info(
            f"Expected AR columns: {expected_cols}. "
            "Normal payments (Cash/Card/Visa/Mada/Amex/MC) share one number; Tabby and Tamara each get their own."
        )

    with data_col:
        st.subheader("Data source")
        source = st.radio(
            "Choose data source",
            ("Upload new files", "Use sample files in this repo"),
            help="Test with bundled samples or use production exports.",
            horizontal=True,
        )

        uploads: Dict[str, Any] = {}
        if source == "Upload new files":
            uploads["line_items"] = st.file_uploader("Line items Excel", type=["xlsx"])
            uploads["payments"] = st.file_uploader("Payments Excel", type=["xlsx"])
            uploads["metadata"] = st.file_uploader("Metadata CSV", type=["csv"])
            uploads["registers"] = st.file_uploader("Registers CSV", type=["csv"])
        else:
            sample_paths, missing = get_sample_paths(Path(__file__).parent)
            for label, name in SAMPLE_FILES.items():
                status = "✅" if name not in missing else "❌ missing"
                st.write(f"{status} {label.replace('_', ' ').title()}: `{name}`")
            if missing:
                st.warning("Sample files are missing. Please upload your own files instead.")

    st.markdown("---")
    st.subheader("Generate templates")

    if st.button("Run template generation", type="primary"):
        working_dir = Path(tempfile.mkdtemp(prefix="fusion_ui_run_"))
        input_paths: Dict[str, str] = {}

        if source == "Upload new files":
            if not all(uploads.values()):
                st.error("Please upload all four files before generating templates.")
                st.stop()
            for key, upload in uploads.items():
                input_paths[key] = persist_upload(upload, working_dir / "inputs")
        else:
            if missing:
                st.error("Sample files are incomplete. Upload your files to continue.")
                st.stop()
            input_paths = sample_paths

        output_dir = working_dir / "ORACLE_FUSION_OUTPUT"

        with st.spinner("Running Oracle Fusion integration..."):
            try:
                log_text, run_result = generate_templates(input_paths, output_dir, prefix, int(starting_seq))
            except Exception as exc:  # pragma: no cover - UI display only
                st.error(f"Template generation failed: {exc}")
                st.stop()

        ar_rows, receipt_rows = summarize_outputs(output_dir)
        ar_df = run_result.get("ar_df", pd.DataFrame())
        ar_stats = run_result.get("ar_stats", {})

        st.success(
            f"Templates generated. AR rows: {ar_rows:,} | Receipts: {receipt_rows:,} | "
            f"Last transaction #: {ar_stats.get('last_transaction_number', starting_seq - 1)}"
        )

        state = update_state_with_run(state, prefix, int(starting_seq), run_result)
        save_state(state)

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Orders", ar_stats.get("invoice_count", ar_rows))
        col_b.metric("Total sales (SAR)", f"{ar_stats.get('total_sales_amount', 0.0):,.2f}")
        col_c.metric("Last transaction #", ar_stats.get("last_transaction_number", starting_seq - 1))

        render_validation(ar_df, ar_stats, expected_cols, ar_rows, receipt_rows)

        archive_path = shutil.make_archive(
            str(working_dir / "fusion_templates"), "zip", str(output_dir)
        )
        with open(archive_path, "rb") as archive:
            zip_bytes = archive.read()

        st.download_button(
            "Download generated templates (.zip)",
            data=zip_bytes,
            file_name="fusion_templates.zip",
            mime="application/zip",
        )

        with st.expander("Run log"):
            st.text_area("Log output", value=log_text, height=240)

        st.caption(f"Files are stored under: {output_dir}")
        st.caption("Mapping guide: PAYMENT_METHOD_MAPPING_GUIDE.txt in the output root.")

    render_history(state)


if __name__ == "__main__":
    main()
