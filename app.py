# -*- coding: utf-8 -*-
from __future__ import annotations

import os, sqlite3, math
from datetime import date, datetime, timedelta
from typing import Tuple

import numpy as np
import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

# ------------------------------
# Config da página
# ------------------------------
st.set_page_config(page_title="Cartão de Crédito — MVP", layout="wide")
BR_DATE_FMT = "%d/%m/%Y"

# ------------------------------
# Login simples (via st.secrets)
# ------------------------------
AUTH_TTL_HOURS = 12

def require_login():
    u = st.session_state.get("_auth_user")
    exp = st.session_state.get("_auth_expire_at")
    if u and exp and datetime.utcnow() < exp:
        with st.sidebar:
            st.caption(f"👋 Olá, {u}")
            if st.button("Sair", use_container_width=True):
                for k in ["_auth_user", "_auth_expire_at"]:
                    st.session_state.pop(k, None)
                st.rerun()
        return True

    st.title("🔒 Acesso restrito")
    user = st.text_input("Usuário")
    pwd = st.text_input("Senha", type="password")
    ok = st.button("Entrar", type="primary")

    if ok:
        good_user = st.secrets.get("APP_USER", "")
        good_pwd = st.secrets.get("APP_PASSWORD", "")
        if user == good_user and pwd == good_pwd and good_user and good_pwd:
            st.session_state["_auth_user"] = user
            st.session_state["_auth_expire_at"] = datetime.utcnow() + timedelta(hours=AUTH_TTL_HOURS)
            st.success("✅ Autenticado!")
            st.rerun()
        else:
            st.error("Usuário ou senha incorretos.")
    st.stop()

require_login()

# ------------------------------
# Utils
# ------------------------------
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
    return ["Alimentação","Mercado","Transporte","Saúde","Lazer","Educação","Moradia","Assinaturas","Vestuário","Tecnologia","Outros"]

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
    """)
    conn.commit()

ensure_schema()

# ------------------------------
# Core
# ------------------------------
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
    return pd.read_sql_query("""
        SELECT t.*, c.name AS card_name
        FROM transactions t JOIN cards c ON c.id=t.card_id
        ORDER BY date(tx_date) DESC, t.id DESC
        """, conn, parse_dates=["tx_date"])

def cycle_range_for(closing_day: int, ref: date) -> Tuple[date, date]:
    if ref.day <= closing_day:
        end = ref.replace(day=closing_day)
        start = (end - relativedelta(months=1)) + timedelta(days=1)
    else:
        start = ref.replace(day=closing_day) + timedelta(days=1)
        first_next = ref.replace(day=1) + relativedelta(months=1)
        end = first_next.replace(day=closing_day)
    return start, end

def invoice_due_date(due_day: int, cycle_end: date) -> date:
    first_next = cycle_end.replace(day=1) + relativedelta(months=1)
    return first_next.replace(day=due_day)

def fatura_atual(card_row: pd.Series, ref: date):
    start, end = cycle_range_for(int(card_row["closing_day"]), ref)
    due = invoice_due_date(int(card_row["due_day"]), end)
    tx = list_transactions()
    mask = (tx["card_id"]==card_row["id"]) & (tx["tx_date"].dt.date>=start) & (tx["tx_date"].dt.date<=end)
    sub = tx[mask].copy()
    total = sub["amount"].sum()
    return start, end, due, sub, total

def add_tx_parcelado(card_id, description, category, amount, purchase_date: date, parcelas=1, tags=None, confirmed=0):
    parcelas = max(1, int(parcelas or 1))
    total = float(amount)
    base = round(total / parcelas, 2)
    acum = 0.0
    for i in range(parcelas):
        eff = purchase_date + relativedelta(months=i)
        desc = f"{description} ({i+1}/{parcelas})" if parcelas > 1 else description
        val = base if i < parcelas - 1 else round(total - acum, 2)
        add_transaction_row(eff, desc, category, int(card_id), val, parcelas, i + 1, confirmed, tags)
        acum += base
    conn.commit()

# ------------------------------
# Sidebar (cards preview + add)
# ------------------------------
with st.sidebar:
    st.header("💳 Cartões")
    cards_df = list_cards()
    if cards_df.empty:
        st.info("Nenhum cartão cadastrado ainda.")
    else:
        for _, card in cards_df.iterrows():
            start, end, due, sub, total = fatura_atual(card, date.today())
            lim = float(card["limit_value"] or 0)
            used_pct = int(total/lim*100) if lim>0 else 0
            st.markdown(
                f"<div style='font-size:0.8rem; line-height:1.2;'><b>{card['name']}</b><br>"
                f"{fmt_brl(total)} / {fmt_brl(lim)} ({used_pct}% usado)</div>",
                unsafe_allow_html=True
            )
            st.progress(min(total/lim, 1.0) if lim>0 else 0)
            st.caption(f"Ciclo: {fmt_br_date(start)} → {fmt_br_date(end)} · Venc.: {fmt_br_date(due)}")
            st.divider()

    with st.form("add_card", clear_on_submit=True):
        st.caption("➕ Novo cartão")
        name = st.text_input("Nome")
        limit_value = st.number_input("Limite (R$)", min_value=0.0, step=100.0, format="%.2f")
        closing_day = st.number_input("Fechamento", 1, 28, 15)
        due_day = st.number_input("Vencimento", 1, 28, 7)
        ok = st.form_submit_button("Adicionar")
        if ok and name.strip():
            add_card(name.strip(), limit_value, closing_day, due_day)
            st.success("Cartão adicionado!")
            st.rerun()

# ------------------------------
# Main
# ------------------------------
st.title("💳 Cartão de Crédito — MVP")
tab_lancar, tab_fatura = st.tabs(["🧾 Lançar compra", "🧮 Fatura"])

with tab_lancar:
    st.subheader("Novo lançamento")
    d = st.date_input("Data da compra", value=date.today())
    desc = st.text_input("Descrição")
    cat = st.selectbox("Categoria", options=categorias_default())
    cards = list_cards()
    if cards.empty:
        st.warning("Cadastre um cartão antes.")
    else:
        cname = st.selectbox("Cartão", options=cards["name"].tolist())
        card_id = int(cards.loc[cards["name"]==cname, "id"].iloc[0])
        amount = st.number_input("Valor (R$)", min_value=0.0, step=0.01, format="%.2f")
        parcelas = st.number_input("Parcelas", min_value=1, max_value=60, value=1)
        if st.button("Adicionar lançamento", type="primary"):
            if amount <= 0 or not desc.strip():
                st.warning("Preencha todos os campos corretamente.")
            else:
                add_tx_parcelado(card_id, desc.strip(), cat, amount, d, parcelas)
                st.success("✅ Lançamento adicionado!")
                st.rerun()

with tab_fatura:
    st.subheader("Fatura atual")
    cards = list_cards()
    if cards.empty:
        st.info("Cadastre um cartão primeiro.")
    else:
        for _, card in cards.iterrows():
            cstart, cend, due, subset, total = fatura_atual(card, date.today())
            header = f"{card['name']} — {fmt_br_date(cstart)} a {fmt_br_date(cend)} | Venc.: {fmt_br_date(due)} | Total: {fmt_brl(total)}"
            with st.expander(header, expanded=False):
                if subset.empty:
                    st.caption("Sem lançamentos no ciclo.")
                else:
                    view = subset[["tx_date","description","category","amount","installments","installment_no","confirmed"]].copy()
                    view.rename(columns={
                        "tx_date":"Data","description":"Descrição","category":"Categoria","amount":"Valor (R$)",
                        "installments":"Parcelas","installment_no":"Parcela","confirmed":"✔"
                    }, inplace=True)
                    view["Data"] = view["Data"].dt.date.map(fmt_br_date)
                    view["Valor (R$)"] = view["Valor (R$)"].apply(fmt_brl)
                    view["✔"] = view["✔"].map({1:"✅",0:"⏳"})
                    st.dataframe(view, use_container_width=True, hide_index=True)
