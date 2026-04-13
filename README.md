# Oracle Fusion Bulk Import Helper

Tools to turn Vend/Odoo exports into Oracle Fusion FBDA templates without losing any columns.

## Quick start
1. Install dependencies: `pip install -r requirements.txt`
2. (Optional) Try the bundled sample files in the repo root.

## Streamlit UI (recommended)
Run `streamlit run streamlit_app.py`, then:
- Set the transaction prefix (e.g., `BULK-ALAJH`) and starting sequence; the app remembers the last number used per prefix.
- Choose **Upload new files** or **Use sample files in this repo**.
- Provide the four inputs (line items, payments, metadata CSV, registers CSV).
- Click **Run template generation** to get AR invoices, receipts, and the mapping guide, with validation checks and a downloadable zip.
- Review the dashboard for transaction numbers used, order count, sales total, and the day-wise run history stored in `state/run_state.json` (ignored by git).

### CRM workspace (beta)
Alongside the generator you now get a lightweight CRM:
- **Dashboard**: pipeline totals (open/won) and recent deals.
- **Contacts**: create and search contacts with company/owner details.
- **Deals**: add deals with stage, amount, close date, and optional contact link; see pipeline by stage.
- **Activities**: log calls/emails/meetings/tasks, link to contacts, and mark them done.
Data persists locally in `state/crm_state.json` (also gitignored).

## CLI
The existing script `Odoo-export-FBDA-template.py` keeps the exact FBDA headers and writes to `ORACLE_FUSION_OUTPUT/` by default. Update the `input_files` mapping near the bottom of the script if you want to point at different files, then run:

```bash
python Odoo-export-FBDA-template.py
```
