# pages/1_Overview_Performance.py
import streamlit as st
import pandas as pd
from utils import load_data, get_sidebar_filters

st.set_page_config(layout="wide")
st.title("📊 Overview de Performance Geral")

# Carrega os dados da aba principal
df_geral = load_data("Dados_Gerais")

if not df_geral.empty:
    df_filtered = get_sidebar_filters(df_geral)
    
    if not df_filtered.empty:
        st.header("KPIs Principais do Período")

        # --- Cálculos dos KPIs ---
        faturamento = df_filtered['faturamento'].sum()
        investimento = df_filtered['investimento'].sum()
        qtde_vendas = df_filtered['quantidade_vendas'].sum()
        unidades_vendidas = df_filtered['unidades_vendidas'].sum()
        visitas = df_filtered['visitas'].sum()
        
        taxa_conversao = qtde_vendas / visitas if visitas > 0 else 0
        acos = df_filtered['acos'].mean() 
        tacos = df_filtered['tacos'].mean()
        roas = faturamento / investimento if investimento > 0 else 0
        roi_media = df_filtered['roi_media'].mean()
        
        # --- Exibição com st.metric ---
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Faturamento", f"R$ {faturamento:,.2f}")
            st.metric("Investimento", f"R$ {investimento:,.2f}")
        with col2:
            st.metric("Quantidade de Vendas", f"{int(qtde_vendas):,}")
            st.metric("Unidades Vendidas", f"{int(unidades_vendidas):,}")
        with col3:
            st.metric("Visitas", f"{int(visitas):,}")
            st.metric("Taxa de Conversão", f"{taxa_conversao:.2%}")
        with col4:
            st.metric("ACOS Médio", f"{acos:.2%}")
            st.metric("TACOS Médio", f"{tacos:.2%}")
        with col5:
            st.metric("ROAS", f"{roas:.2f}")
            st.metric("ROI Média", f"{roi_media:.2f}")

        st.markdown("---")
        
        with st.expander("Responsáveis pelo Projeto (Coordenação)"):
            st.markdown("- **Analista Responsável:** Nome do Analista")
        
        st.subheader("Dados Detalhados do Período")
        st.dataframe(df_filtered)

    else:
        st.info("Nenhum dado encontrado para os filtros selecionados.")
else:
    st.warning("Não foi possível carregar os dados. Verifique a aba 'Dados_Gerais' na sua planilha.")