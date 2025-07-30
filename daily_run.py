import pandas as pd
import requests
import gspread
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo # Importa a biblioteca para fusos horários
import logging
from google.oauth2.service_account import Credentials
import os
import json
from io import StringIO
import toml

# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Módulo de Análise de Estratégia ---
hardcoded_strategy_model_data = [
    {"Nome": "01A - Hig Perforrmance Stage1", "Orçamento": 4500, "ACOS Objetivo": 8, "ACOS": 8},
    {"Nome": "01B - High Performance Stage2", "Orçamento": 1800, "ACOS Objetivo": 7, "ACOS": 7},
    {"Nome": "01C - High Performance Stage3", "Orçamento": 2200, "ACOS Objetivo": 8, "ACOS": 8},
    {"Nome": "Aceleração dinamica 20/8", "Orçamento": 20000, "ACOS Objetivo": 8, "ACOS": 8},
    {"Nome": "Aceleração dinamica 850/22", "Orçamento": 850, "ACOS Objetivo": 22, "ACOS": 22},
    {"Nome": "Aceleração dinamica 20/20", "Orçamento": 20000, "ACOS Objetivo": 20, "ACOS": 20},
    {"Nome": "Aceleração dinamica 10/08", "Orçamento": 10000, "ACOS Objetivo": 8, "ACOS": 8},
    {"Nome": "Alavanca Full", "Orçamento": 1000, "ACOS Objetivo": 45, "ACOS": 45},
    {"Nome": "Anuncio Novo Stage1", "Orçamento": 5000, "ACOS Objetivo": 8, "ACOS": 8},
    {"Nome": "Anuncio Novo Stage2", "Orçamento": 15000, "ACOS Objetivo": 3, "ACOS": 3},
    {"Nome": "Anuncio Novo Stage3", "Orçamento": 10000, "ACOS Objetivo": 8, "ACOS": 8},
    {"Nome": "Acos Elevado", "Orçamento": 50, "ACOS Objetivo": 6, "ACOS": 6},
    {"Nome": "Recorrencia de vendas", "Orçamento": 15, "ACOS Objetivo": 5, "ACOS": 5}
]

def find_best_strategy(campaign_row, strategy_model_df):
    best_match_name = "Nenhuma estrategia recomendada"
    min_acos_diff = float("inf")
    campaign_acos = campaign_row.get("metrics.acos", 0) or 0
    for _, strategy_row in strategy_model_df.iterrows():
        strategy_acos = strategy_row.get("ACOS", 0)
        acos_diff = abs(campaign_acos - strategy_acos)
        if acos_diff < min_acos_diff:
            min_acos_diff = acos_diff
            best_match_name = strategy_row.get("Nome")
    return best_match_name

def analyze_and_consolidate(campaigns_df):
    if campaigns_df.empty: return pd.DataFrame()
    strategy_model_df = pd.DataFrame(hardcoded_strategy_model_data)
    campaigns_df["Estrategia_Recomendada"] = campaigns_df.apply(lambda row: find_best_strategy(row, strategy_model_df), axis=1)
    strategy_data_for_merge = strategy_model_df.rename(columns={"Nome": "Estrategia_Nome_Match", "Orçamento": "Orcamento_Recomendado", "ACOS": "ACOS_Recomendado"})
    consolidated_df = pd.merge(campaigns_df, strategy_data_for_merge, how="left", left_on="Estrategia_Recomendada", right_on="Estrategia_Nome_Match")
    consolidated_df = consolidated_df.rename(columns={"name": "Nome_Campanha", "budget": "Orcamento_Campanha", "metrics.acos": "ACOS_Campanha"})
    return consolidated_df

# --- Módulo de Autenticação Automática ---
def get_new_access_token(client_info):
    url = "https://api.mercadolibre.com/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "refresh_token",
        "client_id": client_info["app_id"],
        "client_secret": client_info["client_secret"],
        "refresh_token": client_info["refresh_token"]
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao renovar o Access Token: {e.response.json()}")
        return None

# --- Módulo de Coleta de Dados ---
class MercadoLivreAdsCollector:
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
        except Exception as e:
            logger.error(f"Erro ao obter ID do usuário: {e}")
            return None

    def get_orders_metrics(self, seller_id, date_from, date_to):
        all_orders, offset = [], 0
        date_from_str, date_to_str = f"{date_from}T00:00:00.000-03:00", f"{date_to}T23:59:59.999-03:00"
        logger.info("Coletando métricas de negócio (pedidos)...")
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
                time.sleep(0.2)
            except Exception as e:
                logger.error(f"Erro ao buscar pedidos: {e}")
                break
        if not all_orders: return {}
        valid_orders = [o for o in all_orders if o.get('status') in ['paid', 'shipped', 'delivered']]
        vendas_brutas = sum(o.get('total_amount', 0) for o in valid_orders)
        return {
            "faturamento_bruto": f"R$ {vendas_brutas:,.2f}",
            "unidades_vendidas": sum(item.get('quantity', 0) for o in valid_orders for item in o.get('order_items', [])),
            "total_de_vendas": len(valid_orders),
            "ticket_medio": f"R$ {vendas_brutas / len(valid_orders):,.2f}" if valid_orders else "R$ 0,00"
        }

    def get_ads_summary_metrics(self, advertiser_id, date_from, date_to):
        logger.info("Buscando resumo de métricas de publicidade...")
        try:
            params = {"date_from": date_from, "date_to": date_to, "metrics_summary": "true", "metrics": "cost,acos"}
            response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"})
            response.raise_for_status()
            return response.json().get("metrics_summary", {})
        except Exception as e:
            logger.error(f"Erro ao obter resumo de publicidade: {e}")
            return None

    def get_all_campaigns_paginated(self, advertiser_id, date_from, date_to):
        all_campaigns, offset = [], 0
        logger.info(f"Coletando dados detalhados das campanhas para o anunciante {advertiser_id}...")
        while True:
            try:
                metrics = ["clicks", "cost", "acos", "total_amount"]
                params = {"limit": 50, "offset": offset, "date_from": date_from, "date_to": date_to, "metrics": ",".join(metrics)}
                response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"})
                response.raise_for_status()
                data = response.json()
                results = data.get('results')
                if not results: break
                all_campaigns.extend(results)
                if offset + 50 >= data.get('paging', {}).get('total', 0): break
                offset += 50
                time.sleep(0.2)
            except Exception as e:
                logger.error(f"Erro ao buscar campanhas: {e}")
                break
        return all_campaigns

    def get_advertisers(self):
        try:
            response = self.session.get(f"{self.base_url}/advertising/advertisers", params={"product_id": "PADS"}, headers={"Api-Version": "1"})
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Erro ao obter anunciantes: {e}")
            return None

# --- Módulo de Exportação com Verificação ---
def export_to_google_sheets(df, sheet_name, worksheet_name, google_creds):
    logger.info(f"Exportando para a aba '{worksheet_name}'...")
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(google_creds, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1", cols=len(df.columns))
        
        initial_row_count = len(worksheet.get_all_records())
        new_header = df.columns.tolist()
        data_to_send = df.fillna("").astype(str).values.tolist()
        try:
            existing_header = worksheet.row_values(1)
        except gspread.exceptions.APIError:
            existing_header = []
        if new_header != existing_header:
            worksheet.clear()
            worksheet.update([new_header] + data_to_send, value_input_option='USER_ENTERED')
            expected_row_count = len(data_to_send)
        else:
            worksheet.append_rows(data_to_send, value_input_option='USER_ENTERED')
            expected_row_count = initial_row_count + len(data_to_send)
        
        time.sleep(3)
        final_row_count = len(worksheet.get_all_records())
        if final_row_count >= expected_row_count:
            logger.info(f"VERIFICAÇÃO BEM-SUCEDIDA: As linhas foram adicionadas na aba '{worksheet_name}'.")
        else:
            logger.error(f"FALHA NA VERIFICAÇÃO: As linhas não foram adicionadas na aba '{worksheet_name}'.")
        return spreadsheet.url
    except Exception as e:
        logger.error(f"ERRO AO EXPORTAR PARA '{worksheet_name}': {e}", exc_info=True)
        return None

# --- Lógica Principal da Automação ---
def main():
    logger.info("Iniciando a execução diária do analisador de campanhas.")
    
    try:
        google_creds_str = os.environ['GOOGLE_CREDENTIALS']
        clients_csv_data = os.environ['MELI_CLIENTS_CSV']
        google_creds = toml.loads(google_creds_str)['google_credentials']
        clients_df = pd.read_csv(StringIO(clients_csv_data))
    except Exception as e:
        logger.error(f"ERRO CRÍTICO ao carregar as credenciais: {e}")
        return

    if clients_df.empty:
        logger.warning("Nenhum cliente encontrado no CSV para processar.")
        return

    brasil_timezone = ZoneInfo("America/Sao_Paulo")
    
    for index, client_info in clients_df.iterrows():
        client_name = client_info["client_name"]
        logger.info("\n" + "="*50 + f"\nProcessando cliente: {client_name}\n" + "="*50)
        
        try:
            access_token = get_new_access_token(client_info)
            if not access_token:
                logger.error(f"Falha ao obter access token para {client_name}. Pulando.")
                continue

            collector = MercadoLivreAdsCollector(access_token)
            end_date = datetime.now(brasil_timezone)
            start_date = end_date - timedelta(days=30)
            date_from_str = start_date.strftime('%Y-%m-%d')
            date_to_str = end_date.strftime('%Y-%m-%d')
            
            advertisers_data = collector.get_advertisers()
            if not advertisers_data or not advertisers_data.get('advertisers'):
                logger.error(f"Nenhum anunciante encontrado para {client_name}. Pulando.")
                continue
            
            advertiser = advertisers_data['advertisers'][0]
            advertiser_id = advertiser['advertiser_id']
            client_name_from_api = advertiser.get('advertiser_name', client_name)
            logger.info(f"Anunciante encontrado: {client_name_from_api} (ID: {advertiser_id})")

            timestamp_geracao = datetime.now(brasil_timezone).strftime('%Y-%m-%d %H:%M:%S')
            periodo_consulta = f"{date_from_str} a {date_to_str}"

            # 1. Métricas Gerais
            user_id = collector.get_user_id()
            if user_id:
                business_metrics = collector.get_orders_metrics(user_id, date_from_str, date_to_str)
                if business_metrics:
                    df_business = pd.DataFrame(list(business_metrics.items()), columns=['Metrica', 'Valor'])
                    df_business.insert(0, 'data_geracao', timestamp_geracao)
                    df_business.insert(1, 'periodo_consulta', periodo_consulta)
                    export_to_google_sheets(df_business, "Dashboard Meli - Resultados", f"{client_name_from_api} - Metricas Gerais", google_creds)
            
            # 2. Métricas de Publicidade
            ads_metrics = collector.get_ads_summary_metrics(advertiser_id, date_from_str, date_to_str)
            if ads_metrics:
                df_ads = pd.DataFrame(list(ads_metrics.items()), columns=['Metrica', 'Valor'])
                df_ads.insert(0, 'data_geracao', timestamp_geracao)
                df_ads.insert(1, 'periodo_consulta', periodo_consulta)
                export_to_google_sheets(df_ads, "Dashboard Meli - Resultados", f"{client_name_from_api} - Metricas Publicidade", google_creds)

            # 3. Análise de Estratégia
            campaigns_data = collector.get_all_campaigns_paginated(advertiser_id, date_from_str, date_to_str)
            if campaigns_data:
                df_campaigns_raw = pd.json_normalize(campaigns_data)
                logger.info("Realizando analise estrategica...")
                df_analysis = analyze_and_consolidate(df_campaigns_raw)
                
                df_analysis.insert(0, 'data_geracao', timestamp_geracao)
                df_analysis.insert(1, 'periodo_consulta', periodo_consulta)
                
                colunas_finais = ['data_geracao', 'periodo_consulta', 'Nome_Campanha', 'status', 'Orcamento_Campanha', 'Orcamento_Recomendado', 'ACOS_Campanha', 'ACOS_Recomendado', 'Estrategia_Recomendada']
                colunas_existentes = [col for col in colunas_finais if col in df_analysis.columns]
                
                export_to_google_sheets(df_analysis[colunas_existentes], "Dashboard Meli - Resultados", f"{client_name_from_api} - Analise de Estrategia", google_creds)
                
                logger.info(f"Processo de análise para {client_name_from_api} concluído.")
            else:
                logger.info(f"Nenhuma campanha encontrada para {client_name_from_api} no período.")

        except Exception as e:
            logger.error(f"ERRO INESPERADO ao processar {client_name}: {e}", exc_info=True)
            continue
            
    logger.info("\nExecução diária finalizada.")

if __name__ == "__main__":
    main()