import io
import json
import os
import shutil
import tempfile
import importlib.util
from datetime import datetime
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st


SAMPLE_FILES = {
    "line_items": "Point of Sale Orders (pos.order) - 2026-04-12T162030.266.xlsx",
    "payments": "Point of Sale Orders (pos.order) - 2026-04-12T162041.258.xlsx",
    "metadata": "FUSION_SALES_METADATA_202604121703.csv",
    "registers": "VENDHQ_REGISTERS_202604121654.csv",
}

STATE_FILE = Path(__file__).parent / "state" / "run_state.json"
CRM_STATE_FILE = Path(__file__).parent / "state" / "crm_state.json"
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


def load_crm_state() -> Dict[str, Any]:
    """Load CRM entities (contacts, deals, activities) with defaults."""
    if CRM_STATE_FILE.exists():
        try:
            with open(CRM_STATE_FILE, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except json.JSONDecodeError:
            pass
    return {
        "contacts": [],
        "deals": [],
        "activities": [],
        "next_contact_id": 1,
        "next_deal_id": 1,
        "next_activity_id": 1,
    }


def save_crm_state(crm_state: Dict[str, Any]) -> None:
    """Persist CRM state."""
    CRM_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CRM_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(crm_state, fh, indent=2)


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


def render_crm_section(crm_state: Dict[str, Any]):
    """Lightweight CRM workspace for contacts, deals, and activities."""
    st.header("CRM workspace (beta)")
    st.caption("Track contacts, deals, and activities alongside your bulk import runs.")

    contacts: List[Dict[str, Any]] = crm_state.get("contacts", [])
    deals: List[Dict[str, Any]] = crm_state.get("deals", [])
    activities: List[Dict[str, Any]] = crm_state.get("activities", [])

    tabs = st.tabs(["Dashboard", "Contacts", "Deals", "Activities"])

    # Dashboard
    with tabs[0]:
        open_deals = [d for d in deals if d.get("stage") not in ("Closed Won", "Closed Lost")]
        pipeline_total = sum(float(d.get("amount", 0) or 0) for d in open_deals)
        won_total = sum(float(d.get("amount", 0) or 0) for d in deals if d.get("stage") == "Closed Won")
        open_activities = [a for a in activities if a.get("status") == "Open"]

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Open pipeline", f"{pipeline_total:,.2f}")
        col_b.metric("Closed won", f"{won_total:,.2f}")
        col_c.metric("Open activities", len(open_activities))

        if deals:
            st.subheader("Recent deals")
            recent = sorted(deals, key=lambda d: d.get("created_at", ""), reverse=True)[:10]
            st.dataframe(
                pd.DataFrame(recent)[["name", "stage", "amount", "close_date", "contact_id"]],
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("Add a deal to see pipeline metrics.")

    # Contacts
    with tabs[1]:
        st.subheader("Contacts")
        with st.form("crm_contact_form"):
            name = st.text_input("Name *")
            email = st.text_input("Email")
            phone = st.text_input("Phone")
            company = st.text_input("Company")
            owner = st.text_input("Owner")
            submitted = st.form_submit_button("Add contact")
            if submitted:
                if not name.strip():
                    st.warning("Name is required.")
                else:
                    contact = {
                        "id": crm_state.get("next_contact_id", 1),
                        "name": name.strip(),
                        "email": email.strip(),
                        "phone": phone.strip(),
                        "company": company.strip(),
                        "owner": owner.strip(),
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    crm_state["next_contact_id"] = contact["id"] + 1
                    contacts.append(contact)
                    save_crm_state(crm_state)
                    st.success(f"Added contact {contact['name']}.")

        search = st.text_input("Search contacts", placeholder="Filter by name, email, company, or owner")
        filtered_contacts = contacts
        if search:
            needle = search.lower()
            filtered_contacts = [
                c
                for c in contacts
                if needle in c.get("name", "").lower()
                or needle in c.get("email", "").lower()
                or needle in c.get("company", "").lower()
                or needle in c.get("owner", "").lower()
            ]
        if filtered_contacts:
            st.dataframe(
                pd.DataFrame(filtered_contacts)[["id", "name", "company", "email", "phone", "owner"]],
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No contacts yet.")

    # Deals
    with tabs[2]:
        st.subheader("Deals")
        contact_options = [(None, "Unassigned")] + [
            (c["id"], f"{c['name']} ({c.get('company','').strip() or 'No company'})") for c in contacts
        ]
        with st.form("crm_deal_form"):
            deal_name = st.text_input("Deal name *")
            stage = st.selectbox(
                "Stage",
                ("Prospecting", "Qualified", "Proposal", "Negotiation", "Closed Won", "Closed Lost"),
                index=0,
            )
            amount = st.number_input("Amount", min_value=0.0, step=100.0, value=0.0)
            close_date = st.date_input("Expected close date", value=datetime.today().date())
            contact_choice = st.selectbox(
                "Link to contact",
                options=contact_options,
                format_func=lambda opt: opt[1],
            )
            deal_submitted = st.form_submit_button("Add deal")
            if deal_submitted:
                if not deal_name.strip():
                    st.warning("Deal name is required.")
                else:
                    deal = {
                        "id": crm_state.get("next_deal_id", 1),
                        "name": deal_name.strip(),
                        "stage": stage,
                        "amount": float(amount),
                        "close_date": str(close_date),
                        "contact_id": contact_choice[0],
                        "created_at": datetime.now().isoformat(timespec="seconds"),
                    }
                    crm_state["next_deal_id"] = deal["id"] + 1
                    deals.append(deal)
                    save_crm_state(crm_state)
                    st.success(f"Added deal {deal['name']}.")

        if deals:
            stage_totals = {}
            for deal in deals:
                stage_totals.setdefault(deal["stage"], 0.0)
                stage_totals[deal["stage"]] += float(deal.get("amount", 0) or 0)
            st.write("Pipeline by stage")
            st.dataframe(
                pd.DataFrame(
                    [{"stage": k, "amount": v} for k, v in stage_totals.items()]
                ).sort_values(by="stage"),
                hide_index=True,
                use_container_width=True,
            )

            st.write("All deals")
            display_deals = []
            for d in deals:
                contact_label = next((c["name"] for c in contacts if c["id"] == d.get("contact_id")), "Unassigned")
                display_deals.append(
                    {
                        "id": d["id"],
                        "name": d["name"],
                        "stage": d["stage"],
                        "amount": d["amount"],
                        "close_date": d["close_date"],
                        "contact": contact_label,
                    }
                )
            st.dataframe(
                pd.DataFrame(display_deals),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No deals yet.")

    # Activities
    with tabs[3]:
        st.subheader("Activities")
        contact_options = [(None, "Unassigned")] + [
            (c["id"], f"{c['name']} ({c.get('company','').strip() or 'No company'})") for c in contacts
        ]
        with st.form("crm_activity_form"):
            activity_type = st.selectbox("Type", ("Call", "Email", "Meeting", "Task"))
            due_date = st.date_input("Due date", value=datetime.today().date())
            note = st.text_area("Notes", height=100)
            contact_choice = st.selectbox(
                "Link to contact",
                options=contact_options,
                format_func=lambda opt: opt[1],
            )
            status = st.selectbox("Status", ("Open", "Done"), index=0)
            activity_submitted = st.form_submit_button("Add activity")
            if activity_submitted:
                activity = {
                    "id": crm_state.get("next_activity_id", 1),
                    "type": activity_type,
                    "note": note.strip(),
                    "due_date": str(due_date),
                    "contact_id": contact_choice[0],
                    "status": status,
                    "created_at": datetime.now().isoformat(timespec="seconds"),
                }
                crm_state["next_activity_id"] = activity["id"] + 1
                activities.append(activity)
                save_crm_state(crm_state)
                st.success("Activity added.")

        open_activities = [a for a in activities if a.get("status") == "Open"]
        if open_activities:
            st.write("Open activities")
            display_activities = []
            for a in open_activities:
                contact_label = next((c["name"] for c in contacts if c["id"] == a.get("contact_id")), "Unassigned")
                display_activities.append(
                    {
                        "id": a["id"],
                        "type": a["type"],
                        "due_date": a["due_date"],
                        "contact": contact_label,
                        "note": a.get("note", ""),
                    }
                )
            st.dataframe(
                pd.DataFrame(display_activities),
                hide_index=True,
                use_container_width=True,
            )

            activity_ids = [a["id"] for a in open_activities]
            selected_id = st.selectbox(
                "Mark an activity as done",
                options=activity_ids,
                format_func=lambda aid: next(
                    (
                        f"{a['type']} for {next((c['name'] for c in contacts if c['id']==a.get('contact_id')), 'Unassigned')} (due {a['due_date']})"
                        for a in open_activities
                        if a["id"] == aid
                    ),
                    f"Activity {aid}",
                ),
            )
            if st.button("Mark selected activity done"):
                for activity in activities:
                    if activity["id"] == selected_id:
                        activity["status"] = "Done"
                        save_crm_state(crm_state)
                        st.success("Activity marked as done.")
                        st.experimental_rerun()
        else:
            st.info("No open activities.")


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
    crm_state = load_crm_state()
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

    st.markdown("---")
    render_crm_section(crm_state)


if __name__ == "__main__":
    main()
