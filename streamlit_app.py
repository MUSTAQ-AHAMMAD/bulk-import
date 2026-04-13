import io
import os
import shutil
import tempfile
import importlib.util
from contextlib import redirect_stdout
from pathlib import Path
from typing import Dict, Tuple

import streamlit as st


SAMPLE_FILES = {
    "line_items": "Point of Sale Orders (pos.order) - 2026-04-12T162030.266.xlsx",
    "payments": "Point of Sale Orders (pos.order) - 2026-04-12T162041.258.xlsx",
    "metadata": "FUSION_SALES_METADATA_202604121703.csv",
    "registers": "VENDHQ_REGISTERS_202604121654.csv",
}


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


def generate_templates(input_paths: Dict[str, str], output_dir: Path) -> str:
    """Run the pipeline and return the captured stdout log."""
    output_dir.mkdir(parents=True, exist_ok=True)
    integration_cls = get_integration_class()
    integration = integration_cls(base_output_dir=str(output_dir))

    log_stream = io.StringIO()
    with redirect_stdout(log_stream):
        integration.run(
            input_paths["line_items"],
            input_paths["payments"],
            input_paths["metadata"],
            input_paths["registers"],
        )
    return log_stream.getvalue()


def summarize_outputs(output_dir: Path) -> Tuple[int, int]:
    """Count AR invoice rows and receipt rows for a quick health check."""
    ar_rows = 0
    receipt_rows = 0

    ar_dir = output_dir / "AR_Invoices"
    if ar_dir.exists():
        latest_ar = sorted(ar_dir.glob("AR_Invoice_Import_*.csv"), key=os.path.getmtime)
        if latest_ar:
            import pandas as pd

            ar_rows = len(pd.read_csv(latest_ar[-1], encoding="utf-8-sig"))

    receipts_dir = output_dir / "Receipts"
    if receipts_dir.exists():
        for receipt_file in receipts_dir.glob("Receipt_Import_*.csv"):
            import pandas as pd

            receipt_rows += len(pd.read_csv(receipt_file, encoding="utf-8-sig"))

    return ar_rows, receipt_rows


def main():
    st.set_page_config(
        page_title="Oracle Fusion Template Generator",
        page_icon="🧾",
        layout="wide",
    )
    st.title("Oracle Fusion Bulk Template Generator")
    st.write(
        "Upload POS exports and metadata to produce FBDA-ready AR invoices and receipts "
        "without losing a single column."
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        source = st.radio(
            "Choose data source",
            ("Upload new files", "Use sample files in this repo"),
            help="You can test with the bundled sample exports or run with your own files.",
        )

    with col2:
        output_folder_name = st.text_input(
            "Output folder name",
            value="ORACLE_FUSION_OUTPUT",
            help="Folder will be created inside a temporary working directory for this run.",
        )

    uploads = {}
    if source == "Upload new files":
        st.subheader("Upload input files")
        uploads["line_items"] = st.file_uploader("Line items Excel", type=["xlsx"])
        uploads["payments"] = st.file_uploader("Payments Excel", type=["xlsx"])
        uploads["metadata"] = st.file_uploader("Metadata CSV", type=["csv"])
        uploads["registers"] = st.file_uploader("Registers CSV", type=["csv"])
    else:
        st.subheader("Sample files")
        sample_paths, missing = get_sample_paths(Path(__file__).parent)
        for label, name in SAMPLE_FILES.items():
            status = "✅" if name not in missing else "❌ missing"
            st.write(f"{status} {label.replace('_', ' ').title()}: `{name}`")
        if missing:
            st.warning(
                "Sample files are missing. Please upload your own files instead."
            )

    expected_cols = get_expected_columns()
    st.info(
        f"AR invoice output keeps all {expected_cols} columns exactly as defined in the FBDA template."
    )

    if st.button("Generate templates", type="primary"):
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

        output_dir = working_dir / output_folder_name

        with st.spinner("Running Oracle Fusion integration..."):
            try:
                log_text = generate_templates(input_paths, output_dir)
            except Exception as exc:  # pragma: no cover - UI display only
                st.error(f"Template generation failed: {exc}")
                st.stop()

        ar_rows, receipt_rows = summarize_outputs(output_dir)
        st.success(
            f"Templates generated. AR rows: {ar_rows:,} | Receipts: {receipt_rows:,}"
        )

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

        st.text_area("Run log", value=log_text, height=260)

        st.caption(f"Files are stored under: {output_dir}")
        st.caption("Mapping guide: PAYMENT_METHOD_MAPPING_GUIDE.txt in the output root.")


if __name__ == "__main__":
    main()
