# -*- coding: utf-8 -*-
from __future__ import annotations

import os, io, sqlite3, math, secrets, importlib.util, re
from datetime import date, datetime, timedelta
from typing import Optional, Tuple, List, Dict

import numpy as np
import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

# ------------------------------
# Configuração da página
# ------------------------------
st.set_page_config(page_title="Cartão de Crédito — MVP", layout="wide")
BR_DATE_FMT = "%d/%m/%Y"

def fmt_br_date(d) -> str:
    if d is None or d == "":
        return ""
    if not isinstance(d, date):
        try:
            d = pd.to_datetime(d).date()
        except Exception:
            return str(d)
    return d.strftime(BR_DATE_FMT)

def fmt_brl(v) -> str:
    try:
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "R$ 0,00"
        s = f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {s}"
    except Exception:
        return str(v)

def categorias_default():
    return [
        "Alimentação","Mercado","Transporte","Saúde","Lazer","Educação",
        "Moradia","Assinaturas","Vestuário","Tecnologia","Outros",
    ]

# ------------------------------
# Banco (SQLite)
# ------------------------------
APP_DIR = os.path.dirname(__file__) if "__file__" in globals() else os.getcwd()
DATA_DIR = os.path.join(APP_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "app.db")

conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.execute("PRAGMA foreign_keys = ON;")

def ensure_schema():
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS cards(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      limit_value REAL DEFAULT 0,
      closing_day INTEGER NOT NULL,
      due_day INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS transactions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      tx_date TEXT NOT NULL,
      description TEXT NOT NULL,
      category TEXT,
      card_id INTEGER NOT NULL,
      amount REAL NOT NULL,
      installments INTEGER NOT NULL DEFAULT 1,
      installment_no INTEGER NOT NULL DEFAULT 1,
      tags TEXT,
      confirmed INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY(card_id) REFERENCES cards(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS bank_statements(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_name TEXT,
      trx_date TEXT NOT NULL,
      description TEXT,
      amount REAL NOT NULL,
      external_id TEXT,
      matched_tx_id INTEGER,
      matched_at TEXT,
      imported_at TEXT,
      batch_id TEXT,
      UNIQUE(external_id)
    );
    """)
    # migrações simples
    def add(table, col, decl):
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in cols:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
    add("bank_statements", "imported_at", "TEXT")
    add("bank_statements", "batch_id", "TEXT")
    conn.commit()

def add_card(name, limit_value, closing_day, due_day):
    conn.execute("INSERT INTO cards(name,limit_value,closing_day,due_day) VALUES(?,?,?,?)",
                 (name.strip(), float(limit_value or 0), int(closing_day), int(due_day)))
    conn.commit()

def list_cards() -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM cards ORDER BY name", conn)

def add_transaction_row(d: date, desc, cat, card_id, amount, inst, inst_no, confirmed=0, tags=None):
    conn.execute(
        """INSERT INTO transactions(tx_date,description,category,card_id,amount,installments,installment_no,tags,confirmed)
           VALUES(?,?,?,?,?,?,?,?,?)""",
        (d.strftime("%Y-%m-%d"), desc, cat, int(card_id), float(amount),
         int(inst), int(inst_no), tags or None, int(confirmed))
    )

def list_transactions() -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT t.*, c.name AS card_name
        FROM transactions t JOIN cards c ON c.id=t.card_id
        ORDER BY date(tx_date) DESC, t.id DESC
        """,
        conn,
        parse_dates=["tx_date"],
    )

def insert_stmt_rows(rows: List[Dict], account_name, batch_id, imported_at) -> int:
    n = 0
    for r in rows:
        try:
            conn.execute(
                """INSERT OR IGNORE INTO bank_statements(account_name,trx_date,description,amount,external_id,matched_tx_id,matched_at,imported_at,batch_id)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (account_name, r["date"].strftime("%Y-%m-%d"), r["desc"], float(r["amount"]),
                 r["fitid"], None, None, imported_at, batch_id)
            )
            n += 1
        except Exception:
            pass
    conn.commit()
    return n

def list_bank_stmt(month: date) -> pd.DataFrame:
    first = month.replace(day=1)
    nextm = first + relativedelta(months=1)
    return pd.read_sql_query(
        """
        SELECT * FROM bank_statements
        WHERE date(trx_date)>=date(?) AND date(trx_date)<date(?)
        ORDER BY date(trx_date) ASC, id ASC
        """,
        conn,
        params=(first.strftime("%Y-%m-%d"), nextm.strftime("%Y-%m-%d")),
        parse_dates=["trx_date"],
    )

# ------------------------------
# Regras de ciclo / vencimento
# ------------------------------
def cycle_range_for(closing_day: int, ref: date) -> Tuple[date, date]:
    if ref.day <= closing_day:
        end = ref.replace(day=closing_day)
        start = (end - relativedelta(months=1)) + timedelta(days=1)
    else:
        start = ref.replace(day=closing_day) + timedelta(days=1)
        first_next = ref.replace(day=1) + relativedelta(months=1)
        end = first_next.replace(day=closing_day)
    return start, end

def next_business_day(d: date, holidays: set[date] | None = None) -> date:
    holidays = holidays or set()
    while d.weekday() >= 5 or d in holidays:
        d += timedelta(days=1)
    return d

def invoice_due_date(due_day: int, cycle_end: date, holidays: set[date] | None = None) -> date:
    first_next = cycle_end.replace(day=1) + relativedelta(months=1)
    target = first_next.replace(day=due_day)
    return next_business_day(target, holidays)

def purchase_first_installment_date(purchase_date: date) -> date:
    return purchase_date  # 1ª parcela na própria data da compra

# ------------------------------
# Serviços
# ------------------------------
def add_tx_parcelado(card_id, description, category, amount, purchase_date: date, parcelas=1, tags=None, confirmed=0):
    parcelas = max(1, int(parcelas or 1))
    # divide com arredondamento e corrige eventual resto na última parcela
    total = float(amount)
    base = round(total / parcelas, 2)
    first = purchase_first_installment_date(purchase_date)
    acum = 0.0
    for i in range(parcelas):
        eff = first + relativedelta(months=i)
        desc = f"{description} ({i + 1}/{parcelas})" if parcelas > 1 else description
        if i < parcelas - 1:
            val = base
            acum += val
        else:
            # última parcela recebe a diferença para fechar o total exato
            val = round(total - acum, 2)
        add_transaction_row(eff, desc, category, int(card_id), val, parcelas, i + 1, confirmed, tags)
    conn.commit()

def fatura_atual(card_row: pd.Series, ref: date):
    start, end = cycle_range_for(int(card_row["closing_day"]), ref)
    due = invoice_due_date(int(card_row["due_day"]), end, set())
    tx = list_transactions()
    mask = (tx["card_id"]==card_row["id"]) & (tx["tx_date"].dt.date>=start) & (tx["tx_date"].dt.date<=end)
    sub = tx[mask].copy()
    total = sub["amount"].sum()
    return start, end, due, sub, total

# ------------------------------
# Prévia do Cartão (Sidebar)
# ------------------------------
def card_preview(card_row: pd.Series, ref: date):
    # infos do ciclo atual
    cstart, cend, due, subset, total = fatura_atual(card_row, ref)
    lim = float(card_row.get("limit_value", 0.0) or 0.0)
    used = float(total or 0.0)
    avail = max(0.0, lim - used)
    util = (used / lim) if lim > 0 else 0.0

    # status dos lançamentos
    qtd_conf = int((subset["confirmed"] == 1).sum()) if not subset.empty else 0
    qtd_pend = int((subset["confirmed"] != 1).sum()) if not subset.empty else 0

    # estilo visual reduzido
    st.markdown(
        f"""
        <div style="
            font-size: 0.8rem;
            line-height: 1.2;
            margin-bottom: 0.2rem;
        ">
            <b>{card_row['name']}</b><br>
            <span style='color:gray'>
                Limite: {fmt_brl(lim)} · Fech.: <b>{int(card_row['closing_day'])}</b> · Venc.: <b>{int(card_row['due_day'])}</b>
            </span>
        </div>
        """,
        unsafe_allow_html=True
    )

    # métricas menores e mais enxutas
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(
            f"""
            <div style="font-size:0.75rem; line-height:1.1;">
                <b>Ciclo:</b> {fmt_br_date(cstart)} → {fmt_br_date(cend)}<br>
                <b>Venc.:</b> {fmt_br_date(due)}
            </div>
            """,
            unsafe_allow_html=True
        )
    with col2:
        st.markdown(
            f"""
            <div style="font-size:0.75rem; line-height:1.1;">
                <b>Usado:</b> {fmt_brl(used)}<br>
                <b>Disp.:</b> {fmt_brl(avail)}
            </div>
            """,
            unsafe_allow_html=True
        )

    st.progress(min(max(util, 0.0), 1.0))
    st.caption(f"✅ {qtd_conf} conc. · ⏳ {qtd_pend} pend.")

# ------------------------------
# OFX (parser com fallback)
# ------------------------------
_HAS_OFX = importlib.util.find_spec("ofxparse") is not None

def _decode(ofx: bytes) -> str:
    for enc in ["utf-8","cp1252","latin-1","iso-8859-1","windows-1252"]:
        try:
            return ofx.decode(enc)
        except UnicodeDecodeError:
            pass
    return ofx.decode("utf-8", errors="ignore")

def _fallback_parse(ofx_bytes: bytes) -> List[Dict]:
    text = _decode(ofx_bytes)
    def fix(tag, s):
        return re.sub(rf"<{tag}>([^<\n\r]+)", rf"<{tag}>\1</{tag}>", s, flags=re.I)
    for t in ["DTPOSTED","TRNAMT","NAME","MEMO","FITID"]:
        text = fix(t, text)
    rows = []
    for blk in re.findall(r"<STMTTRN>(.+?)</STMTTRN>", text, flags=re.I|re.S):
        def g(tag):
            m = re.search(rf"<{tag}>(.*?)</{tag}>", blk, flags=re.I|re.S)
            return m.group(1).strip() if m else ""
        raw = re.sub(r"\D","", g("DTPOSTED"))[:8]
        try:
            d = datetime.strptime(raw, "%Y%m%d").date()
        except Exception:
            continue

        raw_amt = g("TRNAMT").strip()
        # Se tiver vírgula e NÃO tiver ponto => formato BR ("109,99") -> troca vírgula por ponto
        # Caso contrário, assume padrão OFX/US ("109.99") e não mexe
        if ("," in raw_amt) and ("." not in raw_amt):
            amt_txt = raw_amt.replace(".", "").replace(",", ".")
        else:
            amt_txt = raw_amt

        try:
            amt = float(amt_txt)
        except Exception:
            amt = 0.0

        desc = (g("NAME")+" "+g("MEMO")).strip()
        fitid = g("FITID") or f"{d}-{amt}-{desc[:20]}"
        rows.append({"date": d, "amount": amt, "desc": desc, "fitid": fitid})
    return rows

def importar_ofx(ofx_bytes: bytes, account_name: str="Conta"):
    if _HAS_OFX:
        from ofxparse import OfxParser
        try:
            ofx = OfxParser.parse(io.BytesIO(ofx_bytes))
        except Exception:
            ofx = OfxParser.parse(io.StringIO(_decode(ofx_bytes)))
        rows = []
        for acct in ofx.accounts:
            for tr in acct.statement.transactions:
                d = tr.date.date() if hasattr(tr.date, "date") else tr.date
                rows.append({
                    "date": d,
                    "amount": float(tr.amount),
                    "desc": (tr.payee or "") + (f" {tr.memo}" if tr.memo else ""),
                    "fitid": str(tr.id or f"{d}-{tr.amount}")
                })
    else:
        rows = _fallback_parse(ofx_bytes)

    batch_id = datetime.utcnow().strftime("%Y%m%d%H%M%S")+"-"+secrets.token_hex(4)
    imported_at = datetime.utcnow().isoformat(timespec="seconds")
    n = insert_stmt_rows(rows, account_name, batch_id, imported_at)
    return n, batch_id

def ultimo_lote_info():
    row = conn.execute("SELECT batch_id, MAX(imported_at) FROM bank_statements WHERE batch_id IS NOT NULL").fetchone()
    if not row or not row[0]:
        return None
    bid = row[0]
    tot = conn.execute("SELECT COUNT(*) FROM bank_statements WHERE batch_id=?", (bid,)).fetchone()[0]
    conc = conn.execute("SELECT COUNT(*) FROM bank_statements WHERE batch_id=? AND matched_tx_id IS NOT NULL",(bid,)).fetchone()[0]
    return {"batch_id": bid, "total": tot, "conciliadas": conc, "imported_at": row[1]}

def desfazer_ultimo_lote(force=False):
    info = ultimo_lote_info()
    if not info: return 0
    bid = info["batch_id"]
    if force:
        rows = conn.execute("SELECT matched_tx_id FROM bank_statements WHERE batch_id=? AND matched_tx_id IS NOT NULL", (bid,)).fetchall()
        for r in rows:
            conn.execute("UPDATE transactions SET confirmed=0 WHERE id=?", (int(r[0]),))
        conn.execute("UPDATE bank_statements SET matched_tx_id=NULL, matched_at=NULL WHERE batch_id=?", (bid,))
        conn.commit()
    if force:
        conn.execute("DELETE FROM bank_statements WHERE batch_id=?", (bid,))
    else:
        conn.execute("DELETE FROM bank_statements WHERE batch_id=? AND matched_tx_id IS NULL", (bid,))
    conn.commit()
    rest = conn.execute("SELECT COUNT(*) FROM bank_statements WHERE batch_id=?", (bid,)).fetchone()[0]
    return info["total"] - rest

def auto_match(month: date, tol_days=2, tol_value=0.01):
    stmt = list_bank_stmt(month)
    if stmt.empty: return 0
    tx = list_transactions()
    first = month.replace(day=1); nextm = first + relativedelta(months=1)
    tx = tx[(tx["tx_date"].dt.date>=first)&(tx["tx_date"].dt.date<nextm)&(tx["confirmed"]!=1)].copy()
    matched = 0
    for _, s in stmt[stmt["matched_tx_id"].isna()].iterrows():
        d = s["trx_date"].date()
        lo, hi = d - timedelta(days=tol_days), d + timedelta(days=tol_days)
        cand = tx[(tx["tx_date"].dt.date>=lo)&(tx["tx_date"].dt.date<=hi) & (np.isclose(tx["amount"], float(s["amount"]), atol=tol_value))]
        if not cand.empty:
            tid = int(cand.iloc[0]["id"])
            conn.execute("UPDATE bank_statements SET matched_tx_id=?, matched_at=datetime('now') WHERE id=?", (tid, int(s["id"])))
            conn.execute("UPDATE transactions SET confirmed=1 WHERE id=?", (tid,))
            matched += 1
    conn.commit()
    return matched

# ------------------------------
# UI — Sidebar e abas
# ------------------------------
ensure_schema()

with st.sidebar.expander("💳 Cartões", expanded=True):
    cards_df = list_cards()
    if cards_df.empty:
        st.info("Cadastre um cartão para visualizar a prévia aqui.")
    else:
        # prévia por cartão
        for _, card in cards_df.iterrows():
            with st.container():
                card_preview(card, date.today())
                st.divider()

        # exibir tabela completa com um toggle
        show_table = st.checkbox("📋 Mostrar tabela completa", value=False)
        if show_table:
            st.dataframe(
                cards_df.rename(columns={
                    "name": "Cartão",
                    "limit_value": "Limite",
                    "closing_day": "Fechamento",
                    "due_day": "Vencimento"
                }),
                use_container_width=True, hide_index=True
            )

    # formulário para adicionar cartão
    st.markdown("---")
    with st.form("add_card", clear_on_submit=True):
        name = st.text_input("Nome do cartão")
        limit_value = st.number_input("Limite (R$)", min_value=0.0, step=100.0, format="%.2f")
        closing_day = st.number_input("Dia de fechamento", 1, 28, 15)
        due_day = st.number_input("Dia de vencimento", 1, 28, 7)
        ok = st.form_submit_button("Adicionar cartão")
        if ok and name.strip():
            add_card(name.strip(), limit_value, int(closing_day), int(due_day))
            st.success("Cartão adicionado!")
            st.rerun()

st.title("💳 Cartão de Crédito — MVP")
tab_lancar, tab_fatura, tab_conc = st.tabs(["🧾 Lançar compra", "🧮 Fatura (ciclo atual)", "🧾 Conciliação (OFX)"])

# ------------------------------
# Aba: Lançar compra
# ------------------------------
with tab_lancar:
    st.subheader("Novo lançamento")
    left, right = st.columns(2)
    with left:
        d = st.date_input("Data da compra", value=date.today())
        desc = st.text_input("Descrição")
        cat = st.selectbox("Categoria", options=categorias_default())
        tags = st.text_input("Tags (opcional)")
    with right:
        cards = list_cards()
        if cards.empty:
            st.info("Cadastre um cartão ao lado para lançar.")
            card_id = None
        else:
            cname = st.selectbox("Cartão", options=cards["name"].tolist())
            card_id = int(cards.loc[cards["name"]==cname, "id"].iloc[0])
        amount = st.number_input("Valor total (R$)", min_value=0.0, step=0.01, format="%.2f")
        parcelas = st.number_input("Parcelas", min_value=1, max_value=60, value=1)
        st.caption("1ª parcela na data da compra. Demais mensalmente.")

    if st.button("Adicionar lançamento", type="primary", use_container_width=True):
        if not card_id:
            st.warning("Selecione um cartão.")
        elif not desc.strip():
            st.warning("Descrição obrigatória.")
        elif amount <= 0:
            st.warning("Informe um valor válido.")
        else:
            add_tx_parcelado(card_id, desc.strip(), cat, amount, d, int(parcelas), tags, confirmed=0)
            st.success("✅ Lançamento adicionado!")
            st.rerun()

# ------------------------------
# Aba: Fatura (ciclo atual)
# ------------------------------
with tab_fatura:
    st.subheader("Fechamento do ciclo atual (por cartão)")
    cards = list_cards()
    if cards.empty:
        st.info("Cadastre um cartão para ver as faturas.")
    else:
        for _, card in cards.iterrows():
            cstart, cend, due, subset, total = fatura_atual(card, date.today())
            header = f"{card['name']} — ciclo {fmt_br_date(cstart)} a {fmt_br_date(cend)} | Vencimento: {fmt_br_date(due)} | Total: {fmt_brl(total)}"
            with st.expander(header, expanded=False):
                if subset.empty:
                    st.caption("Sem lançamentos no ciclo atual.")
                else:
                    view = subset[["tx_date","description","category","amount","installments","installment_no","tags","confirmed"]].copy()
                    view.rename(columns={"tx_date": "Data","description": "Descrição","category": "Categoria","amount":"Valor (R$)","installments":"Parcelas","installment_no":"Parcela","tags":"Tags","confirmed":"✔"}, inplace=True)
                    view["Data"] = view["Data"].dt.date.map(fmt_br_date)
                    view["Valor (R$)"] = view["Valor (R$)"].apply(fmt_brl)
                    view["✔"] = view["✔"].map({1:"✅",0:"⏳"})
                    st.dataframe(view, use_container_width=True, hide_index=True)

# ------------------------------
# Aba: Conciliação (OFX)
# ------------------------------
with tab_conc:
    st.subheader("Conciliação bancária (OFX)")

    with st.expander("📥 Importar OFX", expanded=True):
        account_name_hint = st.text_input("Nome da conta (opcional)")
        up = st.file_uploader("Selecione um arquivo OFX", type=["ofx"])
        if up is not None and st.button("Importar OFX", type="primary"):
            try:
                n, bid = importar_ofx(up.read(), account_name_hint or "Conta")
                st.success(f"Importadas {n} linhas do OFX. Lote: {bid}")
                st.rerun()
            except Exception as e:
                st.error(f"Falha ao importar OFX: {e}")

    with st.expander("🧨 Desfazer última importação (OFX)", expanded=False):
        info = ultimo_lote_info()
        if not info:
            st.caption("Nenhum lote encontrado.")
        else:
            st.write(f"**Batch:** `{info['batch_id']}` · **Quando:** {info['imported_at']} · **Linhas:** {info['total']} · **Conciliadas:** {info['conciliadas']}")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Desfazer (apenas não conciliadas)"):
                    n = desfazer_ultimo_lote(force=False)
                    st.success(f"Removidas {n} linha(s) não conciliadas do último lote.")
                    st.rerun()
            with c2:
                if st.button("⚠️ Forçar desfazer (inclui conciliadas)"):
                    n = desfazer_ultimo_lote(force=True)
                    st.success(f"Removidas {n} linha(s) do último lote (forçado).")
                    st.rerun()

    # Mês + auto-match
    cmm1, cmm2 = st.columns([1,3])
    with cmm1:
        month_picker = st.date_input("Mês", value=date.today().replace(day=1))
    with cmm2:
        if st.button("🔄 Match automático (±2 dias, ±R$0,01)"):
            n = auto_match(month_picker)
            st.success(f"Conciliadas automaticamente: {n}")
            st.rerun()

    stmt = list_bank_stmt(month_picker)
    tx_all = list_transactions()
    month_tx = tx_all[(tx_all["tx_date"].dt.month==month_picker.month)&(tx_all["tx_date"].dt.year==month_picker.year)].copy()

    # Indicadores topo
    saldo_inicial = 0.0
    creditos = float(stmt[stmt["amount"]>0]["amount"].sum()) if not stmt.empty else 0.0
    debitos = float(stmt[stmt["amount"]<0]["amount"].sum()) if not stmt.empty else 0.0
    saldo_final = saldo_inicial + creditos + debitos
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Saldo Inicial", fmt_brl(saldo_inicial))
    k2.metric("Créditos", fmt_brl(creditos))
    k3.metric("Débitos", fmt_brl(debitos))
    k4.metric("Saldo Final", fmt_brl(saldo_final))

    # Dia selecionado
    if stmt.empty and month_tx.empty:
        st.info("Nada importado/lançado para este mês ainda.")
    else:
        first = month_picker.replace(day=1)
        last_day = (first + relativedelta(months=1) - timedelta(days=1)).day
        cols = st.columns(min(14, last_day))
        sel_day = st.session_state.get("_sel_day", date.today().day)
        for dday in range(1, last_day+1):
            sday = stmt[stmt["trx_date"].dt.day==dday]
            aday = month_tx[month_tx["tx_date"].dt.day==dday]
            diff = round(float(sday["amount"].sum() if not sday.empty else 0.0) - float(aday["amount"].sum() if not aday.empty else 0.0), 2)
            if sday.empty and aday.empty: badge = "⬜"
            elif abs(diff) < 0.01: badge = "🟩"
            else: badge = "🟥"
            label = f"{badge} {dday}"
            if cols[(dday-1)%len(cols)].button(label, key=f"day_{dday}"):
                st.session_state["_sel_day"] = dday
                sel_day = dday
        st.caption("🟩 ok · 🟥 diferença · ⬜ sem movimento")
        st.markdown("---")

        left, right = st.columns(2)
        with left:
            st.markdown("#### Extrato Bancário")
            sday = stmt[stmt["trx_date"].dt.day==sel_day].copy()
            if sday.empty:
                st.caption("Sem itens no extrato neste dia.")
            else:
                sdisp = sday[["id","trx_date","description","amount","matched_tx_id"]].copy()
                sdisp.rename(columns={"trx_date":"Data","description":"Descrição","amount":"Valor","matched_tx_id":"Match"}, inplace=True)
                sdisp["Data"] = sdisp["Data"].dt.date.map(fmt_br_date)
                sdisp["Valor"] = sdisp["Valor"].apply(fmt_brl)
                sdisp["Match"] = sdisp["Match"].apply(lambda v: "✅" if pd.notna(v) else "⏳")
                st.dataframe(sdisp, hide_index=True, use_container_width=True)

        with right:
            st.markdown("#### Movimento lançado")
            aday = month_tx[month_tx["tx_date"].dt.day==sel_day].copy()
            if aday.empty:
                st.caption("Sem lançamentos neste dia.")
            else:
                adisp = aday[["id","tx_date","description","amount","card_name","confirmed"]].copy()
                adisp.rename(columns={"tx_date":"Data","description":"Descrição","amount":"Valor","card_name":"Cartão","confirmed":"✔"}, inplace=True)
                adisp["Data"] = adisp["Data"].dt.date.map(fmt_br_date)
                adisp["Valor"] = adisp["Valor"].apply(fmt_brl)
                adisp["✔"] = adisp["✔"].map({1:"✅",0:"⏳"})
                st.dataframe(adisp, hide_index=True, use_container_width=True)

        st.markdown("---")
        c3, c4, c5 = st.columns([1,1,2])
        with c3:
            st.caption("🔗 Conciliar manualmente")
            stmt_id = st.number_input("ID do extrato", min_value=0, step=1)
            tx_id = st.number_input("ID do lançamento", min_value=0, step=1)
            if st.button("Conciliar"):
                try:
                    conn.execute("UPDATE bank_statements SET matched_tx_id=?, matched_at=datetime('now') WHERE id=?", (int(tx_id), int(stmt_id)))
                    conn.execute("UPDATE transactions SET confirmed=1 WHERE id=?", (int(tx_id),))
                    conn.commit(); st.success("Conciliado!"); st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")
        with c4:
            st.caption("🧹 Desfazer conciliação")
            stmt_id2 = st.number_input("ID do extrato (desfazer)", min_value=0, step=1)
            if st.button("Desconciliar"):
                try:
                    row = conn.execute("SELECT matched_tx_id FROM bank_statements WHERE id=?", (int(stmt_id2),)).fetchone()
                    if row and row[0]:
                        conn.execute("UPDATE transactions SET confirmed=0 WHERE id=?", (int(row[0]),))
                    conn.execute("UPDATE bank_statements SET matched_tx_id=NULL, matched_at=NULL WHERE id=?", (int(stmt_id2),))
                    conn.commit(); st.success("Desfeito!"); st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")
        with c5:
            st.caption("🧾 Criar lançamento a partir do OFX")
            stmt_id3 = st.number_input("ID do extrato → criar lançamento", min_value=0, step=1)
            cards2 = list_cards()
            if cards2.empty:
                st.info("Nenhum cartão disponível.")
            else:
                card_pick = st.selectbox("Cartão para lançar", options=cards2["name"].tolist())
                card_id_pick = int(cards2.loc[cards2["name"]==card_pick, "id"].iloc[0])
                if st.button("Criar lançamento (confirmado)"):
                    try:
                        srow = conn.execute("SELECT trx_date, description, amount FROM bank_statements WHERE id=?", (int(stmt_id3),)).fetchone()
                        if srow:
                            d = pd.to_datetime(srow[0]).date()
                            add_tx_parcelado(card_id_pick, str(srow[1] or "OFX"), "Conciliado", float(srow[2]), d, 1, tags="OFX", confirmed=1)
                            new_tx_id = conn.execute("SELECT id FROM transactions ORDER BY id DESC LIMIT 1").fetchone()[0]
                            conn.execute("UPDATE bank_statements SET matched_tx_id=?, matched_at=datetime('now') WHERE id=?", (int(new_tx_id), int(stmt_id3)))
                            conn.commit(); st.success("Lançamento criado e conciliado!"); st.rerun()
                        else:
                            st.error("ID de extrato não encontrado.")
                    except Exception as e:
                        st.error(f"Erro: {e}")
