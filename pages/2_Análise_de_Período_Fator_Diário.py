# pages/2_Análise_de_Período_Fator_Diário.py
import streamlit as st
import pandas as pd
import plotly.express as px
from utils import load_data, get_sidebar_filters

st.set_page_config(layout="wide")
st.title("📈 Análise de Período Fator (Diário)")

# Usa a mesma fonte de dados diários
df_geral = load_data("Dados_Gerais")

if not df_geral.empty and 'data' in df_geral.columns:
    df_filtered = get_sidebar_filters(df_geral)
    
    if not df_filtered.empty:
        st.header("Análise de Performance Semanal")

        # --- Cálculos Diários ---
        # Adiciona o nome do dia da semana para análise
        df_filtered['dia_semana'] = df_filtered['data'].dt.day_name()
        
        total_vendas = df_filtered['quantidade_vendas'].sum()
        vendas_por_dia = df_filtered.groupby('dia_semana')['quantidade_vendas'].sum().sort_values()
        
        dia_mais_ativo = vendas_por_dia.idxmax() if not vendas_por_dia.empty else "N/D"
        dia_menos_ativo = vendas_por_dia.idxmin() if not vendas_por_dia.empty else "N/D"
        
        dias_fds = ['Saturday', 'Sunday']
        vendas_fds = df_filtered[df_filtered['dia_semana'].isin(dias_fds)]['quantidade_vendas'].sum()
        percentual_fds = vendas_fds / total_vendas if total_vendas > 0 else 0

        # --- KPIs ---
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total de Vendas no Período", f"{int(total_vendas):,}")
        col2.metric("Dia Mais Ativo", dia_mais_ativo)
        col3.metric("Dia Menos Ativo", dia_menos_ativo)
        col4.metric("Vendas no Fim de Semana", f"{percentual_fds:.2%}")

        st.markdown("---")
        
        st.subheader("Gráfico de Análise de Perfil de Vendas")
        
        # Mapeia os dias da semana para português para o gráfico
        dias_map = {
            'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta',
            'Thursday': 'Quinta', 'Friday': 'Sexta', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
        }
        vendas_por_dia.index = vendas_por_dia.index.map(dias_map)
        # Ordena os dias para exibição correta no gráfico
        ordem_dias = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
        vendas_por_dia = vendas_por_dia.reindex(ordem_dias).fillna(0)
        
        fig = px.bar(
            vendas_por_dia, 
            x=vendas_por_dia.index, 
            y='quantidade_vendas',
            labels={'quantidade_vendas': 'Quantidade de Vendas', 'index': 'Dia da Semana'},
            title="Volume de Vendas por Dia da Semana"
        )
        fig.update_layout(xaxis={'categoryorder':'array', 'categoryarray': ordem_dias})
        st.plotly_chart(fig, use_container_width=True)
        
        st.success(f"**Tomada de Decisão:** O dia com maior performance é **{dias_map.get(dia_mais_ativo, 'N/D')}**. Considere focar ou aumentar os investimentos neste dia da semana.")

    else:
        st.info("Nenhum dado encontrado para os filtros selecionados.")
else:
    st.warning("Não foi possível carregar os dados. Verifique a aba 'Dados_Gerais' e a coluna 'data'.")