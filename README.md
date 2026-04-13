# Oracle Fusion Bulk Import Helper

Tools to turn Vend/Odoo exports into Oracle Fusion FBDA templates without losing any columns.

## Quick start
1. Install dependencies: `pip install -r requirements.txt`
2. (Optional) Try the bundled sample files in the repo root.

## Streamlit UI (recommended)
Run `streamlit run streamlit_app.py`, then:
- Choose **Upload new files** or **Use sample files in this repo**.
- Provide the four inputs (line items, payments, metadata CSV, registers CSV).
- Click **Generate templates** and download the zipped outputs (AR invoices, receipts, mapping guide).

## CLI
The existing script `Odoo-export-FBDA-template.py` keeps the exact FBDA headers and writes to `ORACLE_FUSION_OUTPUT/` by default. Update the `input_files` mapping near the bottom of the script if you want to point at different files, then run:

```bash
python Odoo-export-FBDA-template.py
```
