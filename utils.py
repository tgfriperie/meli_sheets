# utils.py
import streamlit as st
import pandas as pd
from gsheetsdb import connect

@st.cache_data(ttl=600)
def load_data(worksheet_name="Dados_Gerais"):
    """Conecta a uma aba específica da planilha e retorna um DataFrame."""
    try:
        conn = connect()
        spreadsheet_url = st.secrets["connections"]["gcs"]["spreadsheet"]
        query = f'SELECT * FROM "{spreadsheet_url}&sheet={worksheet_name}"'
        rows = conn.execute(query, headers=1)
        df = pd.DataFrame(rows)
        return clean_data(df)
    except Exception as e:
        st.error(f"Erro ao carregar dados da aba '{worksheet_name}': {e}")
        return pd.DataFrame()

def clean_data(df):
    """Limpa e converte colunas para os tipos corretos."""
    if df.empty:
        return df
    
    df_clean = df.copy()
    
    # Converte todas as colunas possíveis para numérico, ignorando erros
    for col in df_clean.columns:
        if col != 'data': # Evita converter a coluna de data aqui
             df_clean[col] = pd.to_numeric(df_clean[col].astype(str).str.replace(',', '.'), errors='ignore')

    # Trata especificamente a coluna de data
    if 'data' in df_clean.columns:
        df_clean['data'] = pd.to_datetime(df_clean['data'], errors='coerce')
        
    return df_clean

def get_sidebar_filters(df):
    """Cria e gerencia os filtros na barra lateral."""
    st.sidebar.header("Filtros Globais")
    
    if df.empty or 'data' not in df.columns or df['data'].isnull().all():
        st.sidebar.warning("Não há dados de data para criar o filtro.")
        return pd.DataFrame()

    min_date = df['data'].min().date()
    max_date = df['data'].max().date()
    
    start_date, end_date = st.sidebar.date_input(
        "Selecione o Período:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        format="DD/MM/YYYY"
    )

    selected_clients = []
    if 'cliente' in df.columns:
        all_clients = sorted(df['cliente'].unique())
        selected_clients = st.sidebar.multiselect(
            "Selecione os Clientes:",
            options=all_clients,
            default=all_clients
        )
    
    df_filtered = df[
        (df['data'].dt.date >= start_date) &
        (df['data'].dt.date <= end_date)
    ]

    if selected_clients:
        df_filtered = df_filtered[df_filtered['cliente'].isin(selected_clients)]

    return df_filtered