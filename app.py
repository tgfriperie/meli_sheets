import streamlit as st
import pandas as pd
import requests
import gspread
import time
from datetime import datetime, timedelta
import logging
from google.oauth2.service_account import Credentials

# --- Configuração da Página e Logging ---
st.set_page_config(
    page_title="Exportador e Analisador Meli",
    layout="centered"
)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- Módulo de Análise de Estratégia (Lógica Refinada) ---
hardcoded_strategy_model_data = [
    {"Nome": "01A - Hig Perforrmance Stage1", "Orçamento": 4500, "ACOS Objetivo": 8, "ACOS": 8, "Tipo de Impressão": "Baixa Impressão", "% de impressões ganhas": 10, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 80, "Cliques": 1, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 1},
    {"Nome": "01B - High Performance Stage2", "Orçamento": 1800, "ACOS Objetivo": 7, "ACOS": 7, "Tipo de Impressão": "Media Impressão", "% de impressões ganhas": 25, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 75, "Cliques": 1, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 5},
    {"Nome": "01C - High Performance Stage3", "Orçamento": 2200, "ACOS Objetivo": 8, "ACOS": 8, "Tipo de Impressão": "Impressões elevadas", "% de impressões ganhas": 50, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 50, "Cliques": 0, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 0},
    {"Nome": "Aceleração dinamica 20/8", "Orçamento": 20000, "ACOS Objetivo": 8, "ACOS": 8, "Tipo de Impressão": "Media Impressão", "% de impressões ganhas": 25, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 75, "Cliques": 50, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 100},
    {"Nome": "Aceleração dinamica 850/22", "Orçamento": 850, "ACOS Objetivo": 22, "ACOS": 22, "Tipo de Impressão": "Media Impressão", "% de impressões ganhas": 15, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 85, "Cliques": 100, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 50},
    {"Nome": "Aceleração dinamica 20/20", "Orçamento": 20000, "ACOS Objetivo": 20, "ACOS": 20, "Tipo de Impressão": "Baixa Impressão", "% de impressões ganhas": 50, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 50, "Cliques": 10, "(Investimento / Receitas)": "10 acima", "Unidades vendidas por publicidade": 20, "Quantidade": 1},
    {"Nome": "Aceleração dinamica 10/08", "Orçamento": 10000, "ACOS Objetivo": 8, "ACOS": 8, "Tipo de Impressão": "Impressões elevadas", "% de impressões ganhas": 75, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 25, "Cliques": 100, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 0},
    {"Nome": "Alavanca Full", "Orçamento": 1000, "ACOS Objetivo": 45, "ACOS": 45, "Tipo de Impressão": "Impressões elevadas", "% de impressões ganhas": 50, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 50, "Cliques": 100, "(Investimento / Receitas)": "10 acima", "Unidades vendidas por publicidade": 20, "Quantidade": 0},
    {"Nome": "Anuncio Novo Stage1", "Orçamento": 5000, "ACOS Objetivo": 8, "ACOS": 8, "Tipo de Impressão": "Baixa Impressão", "% de impressões ganhas": 15, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 85, "Cliques": 500, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 50},
    {"Nome": "Anuncio Novo Stage2", "Orçamento": 15000, "ACOS Objetivo": 3, "ACOS": 3, "Tipo de Impressão": "Media Impressão", "% de impressões ganhas": 50, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 50, "Cliques": 500, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 100},
    {"Nome": "Anuncio Novo Stage3", "Orçamento": 10000, "ACOS Objetivo": 8, "ACOS": 8, "Tipo de Impressão": "Impressões elevadas", "% de impressões ganhas": 75, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 25, "Cliques": 1000, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 500},
    {"Nome": "Acos Elevado", "Orçamento": 50, "ACOS Objetivo": 6, "ACOS": 6, "Tipo de Impressão": "Baixa Impressão", "% de impressões ganhas": 50, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 50, "Cliques": 1000, "(Investimento / Receitas)": "10 acima", "Unidades vendidas por publicidade": 20, "Quantidade": 500},
    {"Nome": "Recorrencia de vendas", "Orçamento": 15, "ACOS Objetivo": 5, "ACOS": 5, "Tipo de Impressão": "Impressões elevadas", "% de impressões ganhas": 65, "% de impressões perdidas por orçamento": 0, "% de impressões perdidas por classificação": 35, "Cliques": 1000, "(Investimento / Receitas)": "10 á abaixo", "Unidades vendidas por publicidade": 10, "Quantidade": 1000}
]

def find_best_strategy(campaign_row, strategy_model_df):
    """Encontra a melhor estratégia para uma campanha com base na menor diferença de ACOS."""
    best_match_name = "Nenhuma estrategia recomendada"
    min_acos_diff = float("inf")
    
    # CORREÇÃO: Usa o nome correto da coluna ('metrics.acos') e trata valores nulos.
    campaign_acos = campaign_row.get("metrics.acos", 0) or 0
    
    for _, strategy_row in strategy_model_df.iterrows():
        strategy_acos = strategy_row.get("ACOS", 0)
        acos_diff = abs(campaign_acos - strategy_acos)
        
        if acos_diff < min_acos_diff:
            min_acos_diff = acos_diff
            best_match_name = strategy_row.get("Nome")
            
    return best_match_name

def analyze_and_consolidate(campaigns_df):
    """Aplica a recomendação e consolida os dados para uma análise clara e correta."""
    if campaigns_df.empty:
        return pd.DataFrame()
        
    strategy_model_df = pd.DataFrame(hardcoded_strategy_model_data)
    
    # 1. Recomendar a estratégia
    campaigns_df["Estrategia_Recomendada"] = campaigns_df.apply(
        lambda row: find_best_strategy(row, strategy_model_df), axis=1
    )
    
    # 2. Preparar dados da estratégia para o merge
    strategy_data_for_merge = strategy_model_df.rename(columns={
        "Nome": "Estrategia_Nome_Match",
        "Orçamento": "Orcamento_Recomendado",
        "ACOS": "ACOS_Recomendado"
    })
    
    # 3. Juntar dados da campanha com os da estratégia recomendada
    consolidated_df = pd.merge(
        campaigns_df,
        strategy_data_for_merge,
        how="left",
        left_on="Estrategia_Recomendada",
        right_on="Estrategia_Nome_Match"
    )
    
    # 4. Renomear colunas para clareza e corrigir o nome da coluna de ACOS
    consolidated_df = consolidated_df.rename(columns={
        "name": "Nome_Campanha",
        "budget": "Orcamento_Campanha",
        "metrics.acos": "ACOS_Campanha" # CORREÇÃO CRÍTICA
    })
    
    return consolidated_df


# --- Módulo de Coleta de Dados do Mercado Livre ---
class MercadoLivreAdsCollector:
    """Coletor de dados de anúncios e métricas do Mercado Livre."""
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://api.mercadolibre.com"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"})

    def get_user_id(self):
        try:
            response = self.session.get(f"{self.base_url}/users/me")
            response.raise_for_status()
            return response.json().get('id')
        except requests.exceptions.RequestException as e:
            st.error(f"Erro ao obter ID do usuário: {e}")
            return None

    def get_orders_metrics(self, seller_id, date_from, date_to):
        all_orders = []
        offset = 0
        date_from_str = f"{date_from.strftime('%Y-%m-%d')}T00:00:00.000-03:00"
        date_to_str = f"{date_to.strftime('%Y-%m-%d')}T23:59:59.999-03:00"
        with st.spinner("Coletando métricas de negócio (pedidos)..."):
            while True:
                try:
                    params = {"seller": seller_id, "order.date_created.from": date_from_str, "order.date_created.to": date_to_str, "limit": 50, "offset": offset, "sort": "date_desc"}
                    response = self.session.get(f"{self.base_url}/orders/search", params=params)
                    response.raise_for_status()
                    data = response.json()
                    results = data.get('results', [])
                    if not results: break
                    all_orders.extend(results)
                    if offset + 50 >= data.get('paging', {}).get('total', 0): break
                    offset += 50
                    time.sleep(0.1)
                except requests.exceptions.RequestException as e:
                    st.error(f"Erro ao buscar pedidos: {e}")
                    break
        if not all_orders: return {}
        valid_orders = [o for o in all_orders if o.get('status') in ['paid', 'shipped', 'delivered']]
        vendas_brutas = sum(o.get('total_amount', 0) for o in valid_orders)
        return {"faturamento_bruto": f"R$ {vendas_brutas:,.2f}", "unidades_vendidas": sum(item.get('quantity', 0) for o in valid_orders for item in o.get('order_items', [])), "total_de_vendas": len(valid_orders), "ticket_medio": f"R$ {vendas_brutas / len(valid_orders):,.2f}" if valid_orders else "R$ 0,00"}

    def get_ads_summary_metrics(self, advertiser_id, date_from, date_to):
        with st.spinner("Buscando resumo de métricas de publicidade..."):
            try:
                params = {"date_from": date_from.strftime('%Y-%m-%d'), "date_to": date_to.strftime('%Y-%m-%d'), "metrics_summary": "true", "metrics": "cost,acos"}
                response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"})
                response.raise_for_status()
                return response.json().get("metrics_summary", {})
            except requests.exceptions.RequestException as e:
                st.error(f"Erro ao obter resumo de publicidade: {e}")
                return None

    def get_advertisers(self):
        try:
            response = self.session.get(f"{self.base_url}/advertising/advertisers", params={"product_id": "PADS"}, headers={"Api-Version": "1"})
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            st.error(f"Erro ao obter anunciantes: {e}")
            return None

    def get_all_campaigns_paginated(self, advertiser_id, date_from, date_to):
        all_campaigns = []
        offset = 0
        with st.spinner("Coletando dados detalhados das campanhas..."):
            while True:
                try:
                    metrics = ["clicks", "cost", "acos", "total_amount"]
                    params = {"limit": 50, "offset": offset, "date_from": date_from.strftime('%Y-%m-%d'), "date_to": date_to.strftime('%Y-%m-%d'), "metrics": ",".join(metrics)}
                    response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"})
                    response.raise_for_status()
                    data = response.json()
                    results = data.get('results')
                    if not results: break
                    all_campaigns.extend(results)
                    if offset + 50 >= data.get('paging', {}).get('total', 0): break
                    offset += 50
                    time.sleep(0.1)
                except requests.exceptions.RequestException as e:
                    st.error(f"Erro ao buscar campanhas: {e}")
                    break
        return all_campaigns


# --- Módulo de Exportação para Google Sheets ---
def export_to_google_sheets(df, sheet_name, worksheet_name):
    """Exporta um DataFrame para o Google Sheets, anexando os dados de forma inteligente."""
    with st.spinner(f"Verificando e exportando para a aba '{worksheet_name}'..."):
        try:
            scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(st.secrets["google_credentials"], scopes=scopes)
            client = gspread.authorize(creds)
            spreadsheet = client.open(sheet_name)
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
            except gspread.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1", cols=len(df.columns))
            
            new_header = df.columns.tolist()
            try:
                existing_header = worksheet.row_values(1)
            except gspread.exceptions.APIError:
                existing_header = []

            if new_header != existing_header:
                worksheet.clear()
                worksheet.update([new_header] + df.fillna("").astype(str).values.tolist(), value_input_option='USER_ENTERED')
            else:
                worksheet.append_rows(df.fillna("").astype(str).values.tolist(), value_input_option='USER_ENTERED')
            return spreadsheet.url
        except Exception as e:
            st.error(f"ERRO AO EXPORTAR PARA '{worksheet_name}': {e}")
            return None


# --- Interface do Usuário ---
st.title("Exportador e Analisador de Dados do Mercado Livre")
with st.sidebar:
    st.header("Configuracoes")
    access_token = st.text_input("Access Token do Mercado Livre", type="password")
    end_date_default = datetime.now()
    start_date_default = end_date_default - timedelta(days=30)
    date_range = st.date_input("Selecione o Periodo de Analise", (start_date_default, end_date_default), format="DD/MM/YYYY")
    sheet_name = st.text_input("Nome da Planilha no Google Sheets", "Dashboard Meli - Resultados")
    start_button = st.button("Iniciar Coleta e Exportacao", type="primary", use_container_width=True)

# --- Lógica Principal ---
if start_button:
    if not access_token:
        st.warning("Por favor, insira o Access Token do Mercado Livre.")
    elif not st.secrets.get("google_credentials"):
        st.error("As credenciais do Google Sheets nao foram configuradas nos Secrets do Streamlit.")
    elif not date_range or len(date_range) != 2:
        st.warning("Por favor, selecione um periodo de datas valido.")
    else:
        start_date, end_date = date_range
        st.info(f"Iniciando processo para o periodo de {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}...")
        collector = MercadoLivreAdsCollector(access_token)
        advertisers = collector.get_advertisers()

        if advertisers and advertisers.get('advertisers'):
            advertiser = advertisers['advertisers'][0]
            advertiser_id = advertiser['advertiser_id']
            client_name = advertiser.get('advertiser_name', f"Advertiser_{advertiser_id}")
            st.write(f"Anunciante encontrado: {client_name}")
            timestamp_geracao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            periodo_consulta = f"{start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}"

            # 1. Exportar Métricas Gerais
            user_id = collector.get_user_id()
            if user_id:
                business_metrics = collector.get_orders_metrics(user_id, start_date, end_date)
                if business_metrics:
                    df_business = pd.DataFrame(list(business_metrics.items()), columns=['Metrica', 'Valor'])
                    df_business.insert(0, 'data_geracao', timestamp_geracao)
                    df_business.insert(1, 'periodo_consulta', periodo_consulta)
                    export_to_google_sheets(df_business, sheet_name, f"{client_name} - Metricas Gerais")

            ads_metrics = collector.get_ads_summary_metrics(advertiser_id, start_date, end_date)
            if ads_metrics:
                df_ads = pd.DataFrame(list(ads_metrics.items()), columns=['Metrica', 'Valor'])
                df_ads.insert(0, 'data_geracao', timestamp_geracao)
                df_ads.insert(1, 'periodo_consulta', periodo_consulta)
                export_to_google_sheets(df_ads, sheet_name, f"{client_name} - Metricas Publicidade")

            # 2. Coletar e Analisar Dados de Campanhas
            campaigns_data = collector.get_all_campaigns_paginated(advertiser_id, start_date, end_date)
            if campaigns_data:
                # CORREÇÃO: Usar json_normalize sem separador para obter 'metrics.acos'
                df_campaigns_raw = pd.json_normalize(campaigns_data)
                
                st.info("Realizando analise estrategica das campanhas...")
                df_analysis = analyze_and_consolidate(df_campaigns_raw)
                
                df_analysis.insert(0, 'data_geracao', timestamp_geracao)
                df_analysis.insert(1, 'periodo_consulta', periodo_consulta)
                
                # 3. Selecionar e ordenar as colunas para a planilha final de análise
                colunas_finais = [
                    'data_geracao', 'periodo_consulta', 'Nome_Campanha', 'status', 
                    'Orcamento_Campanha', 'Orcamento_Recomendado',
                    'ACOS_Campanha', 'ACOS_Recomendado',
                    'Estrategia_Recomendada'
                ]
                colunas_existentes = [col for col in colunas_finais if col in df_analysis.columns]
                
                url = export_to_google_sheets(df_analysis[colunas_existentes], sheet_name, f"{client_name} - Analise de Estrategia")

                if url:
                    st.success(f"Processo finalizado! Acesse a planilha aqui: {url}")
            else:
                st.info("Nenhum dado detalhado de campanha foi encontrado para o periodo.")
        else:
            st.error("Nenhum anunciante encontrado. Verifique seu Access Token.")