import pandas as pd
import requests
import gspread
import time
from datetime import datetime, timedelta
import logging
from google.oauth2.service_account import Credentials
import os
import json
from io import StringIO
import toml # Importa a biblioteca toml

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
    # ... (O código da classe continua o mesmo)
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://api.mercadolibre.com"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"})

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
                offset += 50; time.sleep(0.2)
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

# --- Módulo de Exportação ---
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
        
        new_header = df.columns.tolist()
        try:
            existing_header = worksheet.row_values(1)
        except gspread.exceptions.APIError:
            existing_header = []

        if new_header != existing_header:
            logger.info("Cabeçalho diferente. Reescrevendo a aba.")
            worksheet.clear()
            worksheet.update([new_header] + df.fillna("").astype(str).values.tolist(), value_input_option='USER_ENTERED')
        else:
            logger.info("Anexando novas linhas.")
            worksheet.append_rows(df.fillna("").astype(str).values.tolist(), value_input_option='USER_ENTERED')
        return spreadsheet.url
    except Exception as e:
        logger.error(f"ERRO AO EXPORTAR PARA '{worksheet_name}': {e}")
        return None

# --- Lógica Principal da Automação ---
def main():
    logger.info("Iniciando a execução diária do analisador de campanhas.")
    
    try:
        # CORREÇÃO: Usando os nomes exatos do arquivo YAML
        google_creds_str = os.environ['GOOGLE_CREDENTIALS']
        clients_csv_data = os.environ['MELI_CLIENTS_CSV']
        
        google_creds = toml.loads(google_creds_str)['google_credentials']
        clients_df = pd.read_csv(StringIO(clients_csv_data))
        
    except KeyError as e:
        logger.error(f"ERRO: O segredo '{e.args[0]}' não foi encontrado no ambiente do GitHub Actions.")
        return
    except Exception as e:
        logger.error(f"ERRO ao carregar as credenciais: {e}")
        return

    if clients_df.empty:
        logger.warning("Nenhum cliente encontrado no CSV para processar.")
        return

    for index, client_info in clients_df.iterrows():
        client_name = client_info["client_name"]
        logger.info("\n" + "="*50 + f"\nProcessando cliente: {client_name}\n" + "="*50)
        
        try:
            logger.info("Autenticando e obtendo novo access token...")
            access_token = get_new_access_token(client_info)
            if not access_token:
                logger.error(f"Falha ao obter access token para {client_name}. Pulando para o próximo.")
                continue

            collector = MercadoLivreAdsCollector(access_token)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            advertisers_data = collector.get_advertisers()
            if not advertisers_data or not advertisers_data.get('advertisers'):
                logger.error(f"Nenhum anunciante encontrado para {client_name}. Pulando.")
                continue
            
            advertiser = advertisers_data['advertisers'][0]
            advertiser_id = advertiser['advertiser_id']
            client_name_from_api = advertiser.get('advertiser_name', client_name)

            logger.info(f"Anunciante encontrado: {client_name_from_api} (ID: {advertiser_id})")
            
            campaigns_data = collector.get_all_campaigns_paginated(advertiser_id, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))
            
            if campaigns_data:
                df_campaigns_raw = pd.json_normalize(campaigns_data)
                logger.info("Realizando analise estrategica...")
                df_analysis = analyze_and_consolidate(df_campaigns_raw)
                
                timestamp_geracao = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                periodo_consulta = f"{start_date.strftime('%Y-%m-%d')} a {end_date.strftime('%Y-%m-%d')}"
                df_analysis.insert(0, 'data_geracao', timestamp_geracao)
                df_analysis.insert(1, 'periodo_consulta', periodo_consulta)
                
                colunas_finais = ['data_geracao', 'periodo_consulta', 'Nome_Campanha', 'status', 'Orcamento_Campanha', 'Orcamento_Recomendado', 'ACOS_Campanha', 'ACOS_Recomendado', 'Estrategia_Recomendada']
                colunas_existentes = [col for col in colunas_finais if col in df_analysis.columns]
                
                export_to_google_sheets(df_analysis[colunas_existentes], "Dashboard Meli - Resultados", f"{client_name_from_api} - Analise de Estrategia", google_creds)
                
                logger.info(f"Análise para {client_name_from_api} concluída e exportada.")
            else:
                logger.info(f"Nenhuma campanha encontrada para {client_name_from_api} no período.")

        except Exception as e:
            logger.error(f"ERRO INESPERADO ao processar {client_name}: {e}")
            continue
            
    logger.info("\nExecução diária finalizada.")

if __name__ == "__main__":
    main()