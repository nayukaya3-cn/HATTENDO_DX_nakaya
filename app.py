# app.py
# Hattendo Farm - マスター情報登録UI (Streamlit)
# - SQLiteに保存（Root/02_データベース/master.db）
# - Power Query取り込み用に固定仕様でExcel(xlsx)を書き出し（シート名: DATA, 列順固定）
#
# 起動:
#   pip install -r requirements.txt
#   streamlit run app.py

from __future__ import annotations

import json
import sqlite3
import getpass
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ---- Streamlit互換: _divider() が無い古い版向け ----
def _divider() -> None:
    if hasattr(st, "divider"):
        _divider()
    else:
        st.markdown('---')


APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR / "config.json"
SHEET_NAME = "DATA"

@dataclass(frozen=True)
class ColumnDef:
    name: str
    dtype: str = "TEXT"
    required: bool = False
    choices: Optional[List[str]] = None
    help: str = ""

@dataclass(frozen=True)
class MasterDef:
    key: str
    table: str
    folder_name: str
    file_name: str
    id_field: str
    id_prefix: str
    columns: List[ColumnDef]

MASTER_DEFS: Dict[str, MasterDef] = {
    "商品": MasterDef(
        key="商品",
        table="m_product",
        folder_name="マスタ_商品",
        file_name="商品マスタ.xlsx",
        id_field="商品ID",
        id_prefix="P",
        columns=[
            ColumnDef("商品名", required=True),
            ColumnDef("カテゴリ", choices=["パン", "菓子", "デリカ", "資材", "その他"]),
            ColumnDef("単位", choices=["個", "袋", "g", "kg", "本", "箱", "その他"]),
            ColumnDef("有効", choices=["1", "0"], help="1=有効 / 0=無効"),
            ColumnDef("更新日時"),
            ColumnDef("更新者"),
        ],
    ),
    "圃場": MasterDef(
        key="圃場",
        table="m_field",
        folder_name="マスタ_圃場",
        file_name="圃場マスタ.xlsx",
        id_field="圃場ID",
        id_prefix="F",
        columns=[
            ColumnDef("圃場名", required=True),
            ColumnDef("区画"),
            ColumnDef("作物", choices=["ぶどう", "いちご", "野菜", "その他"]),
            ColumnDef("面積", dtype="REAL", help="数値（例: 120.5）"),
            ColumnDef("面積単位", choices=["㎡", "a", "ha"]),
            ColumnDef("有効", choices=["1", "0"]),
            ColumnDef("更新日時"),
            ColumnDef("更新者"),
        ],
    ),
    "作業者": MasterDef(
        key="作業者",
        table="m_worker",
        folder_name="マスタ_作業者",
        file_name="作業者マスタ.xlsx",
        id_field="作業者ID",
        id_prefix="W",
        columns=[
            ColumnDef("氏名", required=True),
            ColumnDef("区分", choices=["自社", "委託", "福祉", "その他"]),
            ColumnDef("所属"),
            ColumnDef("連絡先"),
            ColumnDef("有効", choices=["1", "0"]),
            ColumnDef("更新日時"),
            ColumnDef("更新者"),
        ],
    ),
    "工程": MasterDef(
        key="工程",
        table="m_process",
        folder_name="マスタ_工程",
        file_name="工程マスタ.xlsx",
        id_field="工程ID",
        id_prefix="PR",
        columns=[
            ColumnDef("工程名", required=True),
            ColumnDef("対象", choices=["生鮮", "加工", "共通"]),
            ColumnDef("説明"),
            ColumnDef("チェック項目"),
            ColumnDef("有効", choices=["1", "0"]),
            ColumnDef("更新日時"),
            ColumnDef("更新者"),
        ],
    ),
}

def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_config(cfg: Dict[str, Any]) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")

def db_path(root: Path) -> Path:
    return root / "02_データベース" / "master.db"

def connect_db(root: Path) -> sqlite3.Connection:
    p = db_path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p.as_posix(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_tables(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    for m in MASTER_DEFS.values():
        cols_sql = [f'"{m.id_field}" TEXT PRIMARY KEY']
        for c in m.columns:
            cols_sql.append(f'"{c.name}" {c.dtype}')
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{m.table}" ({", ".join(cols_sql)});')
    conn.commit()

def fetch_df(conn: sqlite3.Connection, m: MasterDef) -> pd.DataFrame:
    # ★ ここが修正ポイント：f-stringの中で別のf-stringを使わない
    col_order = [m.id_field] + [c.name for c in m.columns]
    quoted_cols = ", ".join([f'"{c}"' for c in col_order])
    sql = f'SELECT {quoted_cols} FROM "{m.table}" ORDER BY "{m.id_field}"'
    rows = conn.execute(sql).fetchall()
    if not rows:
        return pd.DataFrame(columns=col_order)
    return pd.DataFrame([dict(r) for r in rows]).reindex(columns=col_order)

def upsert(conn: sqlite3.Connection, m: MasterDef, values: Dict[str, Any]) -> None:
    cols = [m.id_field] + [c.name for c in m.columns]
    data = {k: values.get(k, None) for k in cols}

    placeholders = ", ".join(["?"] * len(cols))
    col_sql = ", ".join([f'"{c}"' for c in cols])
    update_sql = ", ".join([f'"{c}"=excluded."{c}"' for c in cols if c != m.id_field])

    sql = (
        f'INSERT INTO "{m.table}" ({col_sql}) '
        f'VALUES ({placeholders}) '
        f'ON CONFLICT("{m.id_field}") DO UPDATE SET {update_sql};'
    )
    conn.execute(sql, [data[c] for c in cols])
    conn.commit()

def delete_row(conn: sqlite3.Connection, m: MasterDef, row_id: str) -> None:
    conn.execute(f'DELETE FROM "{m.table}" WHERE "{m.id_field}" = ?', (row_id,))
    conn.commit()

def next_id(conn: sqlite3.Connection, m: MasterDef) -> str:
    cur = conn.cursor()
    cur.execute(
        f'SELECT "{m.id_field}" FROM "{m.table}" WHERE "{m.id_field}" LIKE ? '
        f'ORDER BY "{m.id_field}" DESC LIMIT 1',
        (f"{m.id_prefix}%",),
    )
    row = cur.fetchone()
    if row is None:
        n = 1
    else:
        last = str(row[m.id_field])
        suffix = last.replace(m.id_prefix, "", 1)
        try:
            n = int(suffix) + 1
        except Exception:
            n = 1
    return f"{m.id_prefix}{n:06d}"

def export_path(root: Path, year: str, m: MasterDef) -> Path:
    return root / "01_入力データ" / m.folder_name / year / m.file_name

def export_xlsx(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=SHEET_NAME, index=False)

def import_xlsx_to_db(conn: sqlite3.Connection, m: MasterDef, xlsx_path: Path) -> Tuple[int, List[str]]:
    try:
        df = pd.read_excel(xlsx_path, sheet_name=SHEET_NAME, dtype=str)
    except Exception:
        df = pd.read_excel(xlsx_path, sheet_name=0, dtype=str)

    expected_cols = [m.id_field] + [c.name for c in m.columns]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        return 0, [f"列が不足しています: {missing}"]

    df = df.reindex(columns=expected_cols).fillna("")
    count = 0
    for _, r in df.iterrows():
        row_id = str(r[m.id_field]).strip()
        if not row_id:
            continue
        values = {col: ("" if pd.isna(r[col]) else str(r[col]).strip()) for col in expected_cols}
        upsert(conn, m, values)
        count += 1
    return count, []

def input_widget(coldef: ColumnDef, default: str = "") -> Any:
    if coldef.choices:
        idx = coldef.choices.index(default) if default in coldef.choices else 0
        return st.selectbox(coldef.name, options=coldef.choices, index=idx, help=coldef.help)
    if coldef.dtype in ("REAL", "INTEGER"):
        return st.text_input(coldef.name, value=str(default or ""), help=coldef.help, placeholder="例: 120.5")
    return st.text_input(coldef.name, value=str(default or ""), help=coldef.help)

def validate_required(m: MasterDef, values: Dict[str, Any]) -> List[str]:
    errs = []
    for c in m.columns:
        if c.required and str(values.get(c.name, "")).strip() == "":
            errs.append(f"必須: {c.name}")
    return errs

def normalize_types(m: MasterDef, values: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
    errs = []
    out = dict(values)
    for c in m.columns:
        v = out.get(c.name, None)
        if c.dtype == "REAL" and v not in (None, ""):
            try:
                out[c.name] = float(str(v))
            except Exception:
                errs.append(f"{c.name} は数値（小数可）で入力してください。")
        if c.dtype == "INTEGER" and v not in (None, ""):
            try:
                out[c.name] = int(float(str(v)))
            except Exception:
                errs.append(f"{c.name} は整数で入力してください。")
    return out, errs

# -----------------------------
# Streamlit App
# -----------------------------

st.set_page_config(page_title="八天堂ファーム マスター情報登録UI", layout="wide")
st.title("八天堂ファーム：マスター情報登録UI（Power Query連携）")

cfg = load_config()
default_root = cfg.get("root_path", "")
default_year = cfg.get("year", str(datetime.now().year))
default_user = cfg.get("user", getpass.getuser())

with st.sidebar:
    st.header("設定")
    root_path_str = st.text_input(
        "Rootフォルダ（ノウフク連携データ管理）",
        value=default_root,
        placeholder=r"C:\Users\...\Documents\ノウフク連携データ管理",
    )
    year = st.text_input("対象年（フォルダ分け）", value=default_year)
    user_name = st.text_input("更新者名（ログ）", value=default_user)

    _divider()
    master_key = st.selectbox("マスター種別", options=list(MASTER_DEFS.keys()))
    st.caption("※ 出力は固定ファイル名・固定シート名（DATA）です。")

    if st.button("設定を保存"):
        save_config({"root_path": root_path_str, "year": year, "user": user_name})
        st.success("保存しました。")

if not root_path_str.strip():
    st.info("左のサイドバーで Rootフォルダ（ノウフク連携データ管理）のパスを設定してください。")
    st.stop()

root = Path(root_path_str).expanduser()
m = MASTER_DEFS[master_key]

conn = connect_db(root)
ensure_tables(conn)

df = fetch_df(conn, m)
out_path = export_path(root, year, m)

colA, colB, colC = st.columns(3)
colA.metric("マスター", m.key)
colB.metric("件数", len(df))
colC.write(f"出力先: `{out_path}`")

tabs = st.tabs(["登録/更新/削除", "一覧", "取り込み/出力"])

with tabs[0]:
    st.subheader("登録 / 更新 / 削除")
    mode = st.radio("操作", options=["新規登録", "更新", "削除"], horizontal=True)

    existing_ids = df[m.id_field].dropna().astype(str).tolist() if len(df) else []

    if mode == "新規登録":
        new_id = next_id(conn, m)
        st.write(f"新規ID: **{new_id}**（自動採番）")

        with st.form("create_form", clear_on_submit=False):
            values: Dict[str, Any] = {m.id_field: new_id}
            for c in m.columns:
                if c.name in ("更新日時", "更新者"):
                    continue
                values[c.name] = input_widget(c, default="")
            submitted = st.form_submit_button("保存（登録）")

        if submitted:
            values["更新日時"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            values["更新者"] = user_name
            errs = validate_required(m, values)
            values, type_errs = normalize_types(m, values)
            errs += type_errs

            if errs:
                st.error(" / ".join(errs))
            else:
                upsert(conn, m, values)
                df2 = fetch_df(conn, m)
                export_xlsx(df2, out_path)
                st.success(f"登録しました。Power Query用ファイルを更新しました: {out_path}")

    elif mode == "更新":
        if not existing_ids:
            st.warning("更新対象がありません。先に新規登録してください。")
        else:
            target_id = st.selectbox(f"更新する {m.id_field}", options=existing_ids)
            row = df[df[m.id_field].astype(str) == str(target_id)].iloc[0].to_dict()

            with st.form("update_form", clear_on_submit=False):
                st.write(f"{m.id_field}: **{target_id}**（固定）")
                values = {m.id_field: str(target_id)}
                for c in m.columns:
                    if c.name in ("更新日時", "更新者"):
                        continue
                    values[c.name] = input_widget(c, default=str(row.get(c.name, "") or ""))
                submitted = st.form_submit_button("保存（更新）")

            if submitted:
                values["更新日時"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                values["更新者"] = user_name
                errs = validate_required(m, values)
                values, type_errs = normalize_types(m, values)
                errs += type_errs

                if errs:
                    st.error(" / ".join(errs))
                else:
                    upsert(conn, m, values)
                    df2 = fetch_df(conn, m)
                    export_xlsx(df2, out_path)
                    st.success(f"更新しました。Power Query用ファイルを更新しました: {out_path}")

    else:
        if not existing_ids:
            st.warning("削除対象がありません。")
        else:
            target_id = st.selectbox(f"削除する {m.id_field}", options=existing_ids)
            st.error("削除は取り消せません。")
            if st.button("削除を実行"):
                delete_row(conn, m, str(target_id))
                df2 = fetch_df(conn, m)
                export_xlsx(df2, out_path)
                st.success(f"削除しました。Power Query用ファイルを更新しました: {out_path}")

with tabs[1]:
    st.subheader("一覧（列順固定）")
    st.dataframe(df, use_container_width=True)
    st.caption("※ 列順・列名はPower Query連携のため固定運用を推奨します。")

with tabs[2]:
    st.subheader("取り込み / 出力")
    left, right = st.columns(2)

    with left:
        st.markdown("### 取り込み（既存Excel→DB→再出力）")
        up = st.file_uploader("取り込みたいxlsx（DATAシート推奨）", type=["xlsx"])
        if up is not None:
            tmp = APP_DIR / "_upload.xlsx"
            tmp.write_bytes(up.read())
            if st.button("取り込み実行"):
                n, errs = import_xlsx_to_db(conn, m, tmp)
                if errs:
                    st.error("\n".join(errs))
                else:
                    df2 = fetch_df(conn, m)
                    export_xlsx(df2, out_path)
                    st.success(f"{n}件取り込みました。出力も更新しました: {out_path}")

    with right:
        st.markdown("### 出力（DB→Power Query用xlsx）")
        st.write(f"出力ファイル: `{out_path}`")
        if st.button("今すぐ出力"):
            df2 = fetch_df(conn, m)
            export_xlsx(df2, out_path)
            st.success("出力しました。Power Query側で「すべて更新」を実行してください。")

_divider()
st.caption("ローカル運用想定です。複数PCで同時編集する場合はDB共有方式（共有サーバ/クラウド）へ変更してください。")
