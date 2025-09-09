# app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import os

DB_PATH = "inventory.db"
st.set_page_config(page_title="Tyre Inventory Manager", layout="wide")

# ---------------------------
# DB helpers & init
# ---------------------------
def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    conn = get_conn()
    cur = conn.cursor()

    # items: use 'id' as primary key (if inserted with NULL, SQLite will assign one)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        tyre_size TEXT,
        company TEXT,
        series TEXT,
        qty INTEGER DEFAULT 0,
        last_updated TEXT
    )
    """)

    # transactions: audit table references items.id
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER,
        tyre_size TEXT,
        company TEXT,
        series TEXT,
        change INTEGER,
        reason TEXT,
        timestamp TEXT,
        note TEXT,
        FOREIGN KEY(item_id) REFERENCES items(id)
    )
    """)

    conn.commit()
    conn.close()

# ---------------------------
# Excel parsing & import
# ---------------------------
def normalize_cols(df):
    return {c.lower().strip(): c for c in df.columns}

def read_excel_to_df(file_like):
    """
    Parse uploaded file or path into canonical dataframe with columns:
    id, tyre_size, company, series, qty
    Requires at least: ID, Tyre Size, Quantity
    """
    df = pd.read_excel(file_like, engine="openpyxl")
    cols_map = normalize_cols(df)
    required = ['id', 'tyre size', 'quantity']
    if not all(r in cols_map for r in required):
        raise ValueError(f"Excel must contain columns: {required}. Found: {list(df.columns)}")

    rows = []
    for _, row in df.iterrows():
        # id may be missing or NaN -> treat as None (auto assign)
        try:
            excel_id = int(row[cols_map['id']]) if pd.notna(row[cols_map['id']]) else None
        except Exception:
            excel_id = None
        tyre_size = str(row[cols_map['tyre size']]).strip() if pd.notna(row[cols_map['tyre size']]) else None
        company = str(row[cols_map['company']]).strip() if 'company' in cols_map and pd.notna(row[cols_map['company']]) else None
        series = str(row[cols_map['series']]).strip() if 'series' in cols_map and pd.notna(row[cols_map['series']]) else None
        qty = int(row[cols_map['quantity']]) if pd.notna(row[cols_map['quantity']]) else 0
        rows.append({
            "id": excel_id,
            "tyre_size": tyre_size,
            "company": company,
            "series": series,
            "qty": qty
        })
    return pd.DataFrame(rows)

def upsert_items_from_df(df_in):
    """
    Merge/upsert rows from df_in into items table.
    If id provided: if exists -> update qty += qty_from_excel and other fields updated; else insert row with that id.
    If id missing: try to match by (tyre_size, company, series); if found -> qty += qty_from_excel; else insert new row (id auto-assigned).
    Records transactions for each import row.
    """
    conn = get_conn()
    cur = conn.cursor()
    ts = datetime.utcnow().isoformat()

    # Rows with explicit id
    rows_with_id = df_in[df_in['id'].notna()]
    for _, r in rows_with_id.iterrows():
        item_id = int(r['id'])
        tyre_size = r['tyre_size']
        company = r['company']
        series = r['series']
        qty = int(r['qty'])

        cur.execute("SELECT qty FROM items WHERE id = ?", (item_id,))
        found = cur.fetchone()
        if found:
            new_qty = found[0] + qty
            cur.execute("""
                UPDATE items
                SET tyre_size = ?, company = ?, series = ?, qty = ?, last_updated = ?
                WHERE id = ?
            """, (tyre_size, company, series, new_qty, ts, item_id))
        else:
            cur.execute("""
                INSERT INTO items (id, tyre_size, company, series, qty, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (item_id, tyre_size, company, series, qty, ts))

        cur.execute("""
            INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (item_id, tyre_size, company, series, qty, "import_upsert", ts, "import from excel"))

    # Rows without id -> match by signature
    rows_no_id = df_in[df_in['id'].isna()]
    for _, r in rows_no_id.iterrows():
        tyre_size = r['tyre_size']
        company = r['company']
        series = r['series']
        qty = int(r['qty'])

        cur.execute("""
            SELECT id, qty FROM items
            WHERE tyre_size = ? AND (company = ? OR (company IS NULL AND ? IS NULL)) AND (series = ? OR (series IS NULL AND ? IS NULL))
        """, (tyre_size, company, company, series, series))
        found = cur.fetchone()
        if found:
            item_id, cur_qty = found
            new_qty = cur_qty + qty
            cur.execute("UPDATE items SET qty = ?, last_updated = ? WHERE id = ?", (new_qty, ts, item_id))
            cur.execute("""
                INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (item_id, tyre_size, company, series, qty, "import_merge_signature", ts, "imported by signature"))
        else:
            # insert new (id will be auto-assigned)
            cur.execute("""
                INSERT INTO items (tyre_size, company, series, qty, last_updated)
                VALUES (?, ?, ?, ?, ?)
            """, (tyre_size, company, series, qty, ts))
            new_id = cur.lastrowid
            cur.execute("""
                INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_id, tyre_size, company, series, qty, "import_new", ts, "new row from excel"))

    conn.commit()
    conn.close()

def replace_db_with_df(df_in):
    """
    Destructive: delete all existing items & transactions and replace with df_in rows.
    Uses provided id if present (will be preserved), otherwise auto-assigns.
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM transactions")
    cur.execute("DELETE FROM items")
    ts = datetime.utcnow().isoformat()

    for _, r in df_in.iterrows():
        excel_id = int(r['id']) if pd.notna(r['id']) else None
        tyre_size = r['tyre_size']
        company = r['company']
        series = r['series']
        qty = int(r['qty'])

        if excel_id is not None:
            cur.execute("""
                INSERT INTO items (id, tyre_size, company, series, qty, last_updated)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (excel_id, tyre_size, company, series, qty, ts))
            item_id = excel_id
        else:
            cur.execute("""
                INSERT INTO items (tyre_size, company, series, qty, last_updated)
                VALUES (?, ?, ?, ?, ?)
            """, (tyre_size, company, series, qty, ts))
            item_id = cur.lastrowid

        cur.execute("""
            INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (item_id, tyre_size, company, series, qty, "replace_import", ts, "replace db from excel"))

    conn.commit()
    conn.close()

# ---------------------------
# Add / Remove stock ops
# ---------------------------
def get_items_df():
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM items ORDER BY id", conn)
    conn.close()
    return df

def add_stock(item_id, tyre_size, company, series, qty, note=None):
    conn = get_conn()
    cur = conn.cursor()
    ts = datetime.utcnow().isoformat()

    if item_id is not None:
        # ensure item exists; update qty or insert if missing
        cur.execute("SELECT qty FROM items WHERE id = ?", (item_id,))
        r = cur.fetchone()
        if r:
            new_qty = r[0] + qty
            cur.execute("UPDATE items SET qty = ?, tyre_size = ?, company = ?, series = ?, last_updated = ? WHERE id = ?",
                        (new_qty, tyre_size, company, series, ts, item_id))
        else:
            cur.execute("INSERT INTO items (id, tyre_size, company, series, qty, last_updated) VALUES (?, ?, ?, ?, ?, ?)",
                        (item_id, tyre_size, company, series, qty, ts))
    else:
        # insert new and get generated id
        cur.execute("INSERT INTO items (tyre_size, company, series, qty, last_updated) VALUES (?, ?, ?, ?, ?)",
                    (tyre_size, company, series, qty, ts))
        item_id = cur.lastrowid

    cur.execute("INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (item_id, tyre_size, company, series, qty, "stock_in", ts, note))
    conn.commit()
    conn.close()
    return item_id

def remove_stock_by_id(item_id, qty, reason="sale", note=None):
    conn = get_conn()
    cur = conn.cursor()
    ts = datetime.utcnow().isoformat()

    cur.execute("SELECT qty, tyre_size, company, series FROM items WHERE id = ?", (item_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        raise ValueError("Item ID not found.")
    cur_qty, tyre_size, company, series = row[0], row[1], row[2], row[3]
    if qty > cur_qty:
        conn.close()
        raise ValueError(f"Not enough stock. Current: {cur_qty}, requested: {qty}")
    new_qty = cur_qty - qty
    cur.execute("UPDATE items SET qty = ?, last_updated = ? WHERE id = ?", (new_qty, ts, item_id))
    cur.execute("INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (item_id, tyre_size, company, series, -qty, reason, ts, note))
    conn.commit()
    conn.close()

def find_item_by_signature(tyre_size, company, series):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, qty, tyre_size, company, series FROM items
        WHERE tyre_size = ? AND (company = ? OR (company IS NULL AND ? IS NULL)) AND (series = ? OR (series IS NULL AND ? IS NULL))
    """, (tyre_size, company, company, series, series))
    row = cur.fetchone()
    conn.close()
    return row  # (id, qty, tyre_size, company, series) or None

def remove_stock_by_signature(tyre_size, company, series, qty, reason="sale", note=None):
    found = find_item_by_signature(tyre_size, company, series)
    if not found:
        raise ValueError("Item not found by signature.")
    item_id, cur_qty = found[0], found[1]
    if qty > cur_qty:
        raise ValueError(f"Not enough stock. Current: {cur_qty}, requested: {qty}")
    conn = get_conn()
    cur = conn.cursor()
    ts = datetime.utcnow().isoformat()
    new_qty = cur_qty - qty
    cur.execute("UPDATE items SET qty = ?, last_updated = ? WHERE id = ?", (new_qty, ts, item_id))
    cur.execute("INSERT INTO transactions (item_id, tyre_size, company, series, change, reason, timestamp, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (item_id, found[2], found[3], found[4], -qty, reason, ts, note))
    conn.commit()
    conn.close()

# ---------------------------
# Backup / exports
# ---------------------------
def backup_db_to_excel(output_path="inventory_backup.xlsx"):
    df = get_items_df()
    df.to_excel(output_path, index=False)
    return output_path

def export_transactions(output_path="transactions.xlsx"):
    conn = get_conn()
    df = pd.read_sql_query("SELECT * FROM transactions ORDER BY timestamp DESC", conn)
    conn.close()
    df.to_excel(output_path, index=False)
    return output_path

# ---------------------------
# UI
# ---------------------------
init_db()


col1, col2 = st.columns([1, 15])  # adjust width ratios
with col1:
    st.image("logo.png", width=100)   # local file works here
with col2:
    st.markdown("<h1>Tread&Co. Inventory Manager</h1>", unsafe_allow_html=True)

col1, col2 = st.columns([1, 2])

with col1:
    st.header("Import Excel into DB")
    uploaded_file = st.file_uploader("Upload inventory Excel (.xlsx)", type=["xlsx"])
    st.markdown("**Or** place `inventory.xlsx` in the app folder and click the button below.")
    import_mode = st.selectbox("Import mode", ("Merge / Upsert (recommended)", "Replace DB with Excel (destructive)"))

    if uploaded_file is not None:
        try:
            df_excel = read_excel_to_df(uploaded_file)
            st.success(f"Preview: {len(df_excel)} rows parsed from uploaded Excel.")
            st.dataframe(df_excel.head(20))
            if st.button("Start import from uploaded file"):
                if import_mode.startswith("Merge"):
                    upsert_items_from_df(df_excel)
                    st.success("Merge/Upsert import completed.")
                else:
                    st.warning("You chose destructive replace — this will wipe existing items & transactions.")
                    if st.button("CONFIRM AND REPLACE DB"):
                        replace_db_with_df(df_excel)
                        st.success("DB replaced with Excel contents.")
        except Exception as e:
            st.error(f"Error parsing uploaded Excel: {e}")

    if st.button("Import from inventory.xlsx in app folder"):
        path = "inventory.xlsx"
        if not os.path.exists(path):
            st.error("No inventory.xlsx found in app folder.")
        else:
            try:
                df_excel = read_excel_to_df(path)
                st.success(f"Parsed {len(df_excel)} rows from {path}.")
                if import_mode.startswith("Merge"):
                    upsert_items_from_df(df_excel)
                    st.success("Merge/Upsert import from file completed.")
                else:
                    st.warning("You chose destructive replace — this will wipe existing items & transactions.")
                    if st.button("CONFIRM AND REPLACE DB (file)"):
                        replace_db_with_df(df_excel)
                        st.success("DB replaced with Excel contents.")
            except Exception as e:
                st.error(f"Error reading {path}: {e}")

    st.markdown("---")
    st.header("Stock (Add)")
    with st.form("add_form"):
        id_input = st.text_input("ID (leave blank to auto-generate new id)", help="If you want to assign a specific ID, enter it. Otherwise leave blank.")
        tyre_size = st.text_input("Tyre Size (e.g., '155 70 R 13')")
        company = st.text_input("Company (optional)")
        series = st.text_input("Series (optional)")
        qty_add = st.number_input("Quantity to add", min_value=1, value=1, step=1)
        note = st.text_area("Note (optional)")
        submitted = st.form_submit_button("Add to stock")
    if submitted:
        try:
            item_id = int(id_input.strip()) if id_input.strip() else None
            if not tyre_size:
                st.error("Tyre Size is required.")
            else:
                new_id = add_stock(item_id, tyre_size.strip(), company.strip() or None, series.strip() or None, int(qty_add), note.strip() or None)
                st.success(f"Added {qty_add} units. Item ID: {new_id}")
        except Exception as e:
            st.error(f"Error: {e}")

    st.markdown("---")
    st.header("Sell / Remove stock")
    with st.form("remove_form"):
        rem_mode = st.radio("Remove by", ("ID", "Tyre Size + Company + Series"), index=0)
        if rem_mode == "ID":
            rem_id = st.text_input("ID to remove")
            qty_rm = st.number_input("Quantity to remove", min_value=1, value=1, step=1, key="rm_id_qty")
            reason = st.selectbox("Reason", ["sale", "damage", "return", "other"], key="rm_id_reason")
            note_rm = st.text_area("Note (optional)", key="rm_id_note")
        else:
            rem_tyre = st.text_input("Tyre Size (e.g., '155 70 R 13')", key="rem_tyre")
            rem_company = st.text_input("Company", key="rem_company")
            rem_series = st.text_input("Series (optional)", key="rem_series")
            qty_rm = st.number_input("Quantity to remove", min_value=1, value=1, step=1, key="rm_sig_qty")
            reason = st.selectbox("Reason", ["sale", "damage", "return", "other"], key="rm_sig_reason")
            note_rm = st.text_area("Note (optional)", key="rm_sig_note")
        rem_sub = st.form_submit_button("Remove from stock")
    if rem_sub:
        try:
            if rem_mode == "ID":
                if not rem_id.strip():
                    st.error("Provide ID to remove.")
                else:
                    remove_stock_by_id(int(rem_id.strip()), int(qty_rm), reason, note_rm.strip() or None)
                    st.success(f"Removed {qty_rm} units from ID {rem_id.strip()}.")
            else:
                if not rem_tyre:
                    st.error("Provide Tyre Size for signature removal.")
                else:
                    remove_stock_by_signature(rem_tyre.strip(), rem_company.strip() or None, rem_series.strip() or None, int(qty_rm), reason, note_rm.strip() or None)
                    st.success(f"Removed {qty_rm} units ({rem_tyre} / {rem_company}).")
        except Exception as e:
            st.error(f"Error: {e}")

    st.markdown("---")
    st.header("Backup / Exports")
    if st.button("Backup current inventory to Excel"):
        out = backup_db_to_excel()
        st.success(f"Backup saved to {out}")
        with open(out, "rb") as f:
            st.download_button("Download backup", data=f, file_name=out)
    if st.button("Export transactions to Excel"):
        out = export_transactions()
        st.success(f"Transactions exported to {out}")
        with open(out, "rb") as f:
            st.download_button("Download transactions", data=f, file_name=out)

with col2:
    st.header("Inventory table")
    try:
        df = get_items_df()
        st.dataframe(df)
        st.markdown("### Quick stats")
        total_items = int(df['qty'].sum()) if not df.empty else 0
        distinct = len(df)
        st.metric("Total units in stock", total_items)
        st.metric("Distinct tyre entries", distinct)

        st.markdown("### Search / Filter")
        q = st.text_input("Search by ID, Tyre Size or Company")
        if q:
            df_f = df[df['id'].astype(str).str.contains(q, case=False, na=False) |
                      df['tyre_size'].str.contains(q, case=False, na=False) |
                      df['company'].astype(str).str.contains(q, case=False, na=False)]
            st.dataframe(df_f)
        else:
            st.markdown("#### Low stock (qty <= 5)")
            low = df[df['qty'] <= 5].sort_values("qty")
            st.dataframe(low)
    except Exception as e:
        st.error(f"Error reading DB: {e}")

st.markdown("---")
st.write("App last updated:", datetime.utcnow().isoformat())
