import pandas as pd
import requests
import gspread
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
from google.oauth2.service_account import Credentials
import os
from io import StringIO
import toml

# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Módulos de Análise, Autenticação e Coleta ---

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
    if campaigns_df.empty:
        return pd.DataFrame()
    
    # --- CORREÇÃO: A variável é definida aqui dentro ---
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
    strategy_model_df = pd.DataFrame(hardcoded_strategy_model_data)

    campaigns_df["Estrategia_Recomendada"] = campaigns_df.apply(lambda row: find_best_strategy(row, strategy_model_df), axis=1)
    strategy_data_for_merge = strategy_model_df.rename(columns={"Nome": "Estrategia_Nome_Match", "Orçamento": "Orcamento_Recomendado", "ACOS": "ACOS_Recomendado"})
    consolidated_df = pd.merge(campaigns_df, strategy_data_for_merge, how="left", left_on="Estrategia_Recomendada", right_on="Estrategia_Nome_Match")
    consolidated_df = consolidated_df.rename(columns={"name": "Nome_Campanha", "budget": "Orcamento_Campanha", "metrics.acos": "ACOS_Campanha"})
    return consolidated_df

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

class MercadoLivreAdsCollector:
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://api.mercadolibre.com"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"})
        self.timeout = 30

    def get_user_id(self):
        try:
            response = self.session.get(f"{self.base_url}/users/me", timeout=self.timeout)
            response.raise_for_status()
            return response.json().get('id')
        except Exception as e:
            logger.error(f"Erro ao obter ID do usuário: {e}")
            return None

    def get_orders_metrics(self, seller_id, date_str):
        date_from_str, date_to_str = f"{date_str}T00:00:00.000-03:00", f"{date_str}T23:59:59.999-03:00"
        try:
            params = {"seller": seller_id, "order.date_created.from": date_from_str, "order.date_created.to": date_to_str, "sort": "date_desc"}
            response = self.session.get(f"{self.base_url}/orders/search", params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            valid_orders = [o for o in data.get('results', []) if o.get('status') in ['paid', 'shipped', 'delivered']]
            if not valid_orders: return {}
            vendas_brutas = sum(o.get('total_amount', 0) for o in valid_orders)
            return {
                "faturamento_bruto": f"R$ {vendas_brutas:,.2f}",
                "unidades_vendidas": sum(item.get('quantity', 0) for o in valid_orders for item in o.get('order_items', [])),
                "total_de_vendas": len(valid_orders),
                "ticket_medio": f"R$ {vendas_brutas / len(valid_orders):,.2f}" if valid_orders else "R$ 0,00"
            }
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos: {e}")
            return {}

    def get_ads_summary_metrics(self, advertiser_id, date_str):
        try:
            params = {"date_from": date_str, "date_to": date_str, "metrics_summary": "true", "metrics": "cost,acos,direct_amount,indirect_amount,total_amount"}
            response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"}, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get("metrics_summary", {})
        except Exception as e:
            logger.error(f"Erro ao obter resumo de publicidade: {e}")
            return {}

    def get_all_campaigns_paginated(self, advertiser_id, date_str):
        all_campaigns, offset = [], 0
        while True:
            try:
                metrics = ["clicks", "cost", "acos", "total_amount"]
                params = {"limit": 50, "offset": offset, "date_from": date_str, "date_to": date_str, "metrics": ",".join(metrics)}
                response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"}, timeout=self.timeout)
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
            response = self.session.get(f"{self.base_url}/advertising/advertisers", params={"product_id": "PADS"}, headers={"Api-Version": "1"}, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Erro ao obter anunciantes: {e}")
            return None

def export_to_google_sheets(df, sheet_name, worksheet_name, google_creds, update_key_cols=None):
    if update_key_cols is None:
        update_key_cols = []
    logger.info(f"Exportando/Atualizando para a aba '{worksheet_name}'...")
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(google_creds, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(sheet_name)
        
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1", cols=len(df.columns))
            worksheet.update([df.columns.tolist()], value_input_option='USER_ENTERED')
        
        if not update_key_cols:
            worksheet.append_rows(df.fillna("").astype(str).values.tolist(), value_input_option='USER_ENTERED')
            logger.info(f"SUCESSO: Novas linhas adicionadas na aba '{worksheet_name}'.")
            return

        all_records = worksheet.get_all_records()
        existing_data_df = pd.DataFrame(all_records)
        
        new_rows_to_append = []
        
        for index, new_row in df.iterrows():
            match_index = -1
            if not existing_data_df.empty:
                condition = pd.Series([True] * len(existing_data_df))
                for col in update_key_cols:
                    # Garante que a comparação seja feita com strings para evitar erros de tipo
                    if col in existing_data_df.columns:
                        condition &= (existing_data_df[col].astype(str) == str(new_row[col]))
                    else:
                        condition = pd.Series([False] * len(existing_data_df)) # Se a coluna chave não existe, não há match
                        break

                matched_rows = existing_data_df[condition]
                if not matched_rows.empty:
                    match_index = matched_rows.index[0]

            if match_index != -1:
                row_to_update = match_index + 2
                header = worksheet.row_values(1)
                ordered_new_row_values = [new_row.get(col, "") for col in header]
                worksheet.update(f"A{row_to_update}", [ordered_new_row_values], value_input_option='USER_ENTERED')
                logger.info(f"SUCESSO: Linha {row_to_update} atualizada na aba '{worksheet_name}'.")
            else:
                new_rows_to_append.append(new_row)

        if new_rows_to_append:
            df_to_append = pd.DataFrame(new_rows_to_append)
            header = worksheet.row_values(1)
            # Alinha as colunas do novo DF com as do header da planilha
            df_aligned = pd.DataFrame(columns=header)
            for col in df_to_append.columns:
                if col in df_aligned.columns:
                    df_aligned[col] = df_to_append[col]
            worksheet.append_rows(df_aligned.fillna("").values.tolist(), value_input_option='USER_ENTERED')
            logger.info(f"SUCESSO: {len(new_rows_to_append)} nova(s) linha(s) adicionada(s) na aba '{worksheet_name}'.")

    except Exception as e:
        logger.error(f"ERRO AO EXPORTAR PARA '{worksheet_name}': {e}", exc_info=True)

def main():
    logger.info("Iniciando a execução da atualização em tempo real.")
    
    try:
        if os.path.exists('.streamlit/secrets.toml'):
            secrets = toml.load('.streamlit/secrets.toml')
            google_creds = secrets['google_credentials']
            with open('clients.csv', 'r') as f:
                 clients_csv_data = f.read()
        else:
            google_creds_str = os.environ['GOOGLE_CREDENTIALS']
            clients_csv_data = os.environ['MELI_CLIENTS_CSV']
            google_creds = toml.loads(google_creds_str)['google_credentials']
        
        clients_df = pd.read_csv(StringIO(clients_csv_data))
    except Exception as e:
        logger.error(f"ERRO CRÍTICO ao carregar credenciais: {e}")
        return

    brasil_timezone = ZoneInfo("America/Sao_Paulo")
    today = datetime.now(brasil_timezone)
    date_str = today.strftime('%Y-%m-%d')
    
    logger.info(f"Coletando dados para o dia de hoje: {date_str}")

    for index, client_info in clients_df.iterrows():
        client_name = client_info["client_name"]
        logger.info(f"\n--- Processando cliente: {client_name} ---")
        
        try:
            access_token = get_new_access_token(client_info)
            if not access_token:
                logger.error(f"Falha ao obter access token para {client_name}. Pulando.")
                continue
                
            collector = MercadoLivreAdsCollector(access_token)
            advertisers_data = collector.get_advertisers()
            
            if not advertisers_data or not advertisers_data.get('advertisers'):
                logger.error(f"Nenhum anunciante encontrado para {client_name}. Pulando.")
                continue
            
            advertiser = advertisers_data['advertisers'][0]
            advertiser_id = advertiser['advertiser_id']
            client_name_from_api = advertiser.get('advertiser_name', client_name)
            timestamp_geracao = datetime.now(brasil_timezone).strftime('%Y-%m-%d %H:%M:%S')

            user_id = collector.get_user_id()
            business_metrics = collector.get_orders_metrics(user_id, date_str) if user_id else {}
            ads_metrics = collector.get_ads_summary_metrics(advertiser_id, date_str)
            
            consolidated_data = {
                "data_geracao": timestamp_geracao,
                "periodo_consulta": date_str,
                "cliente": client_name_from_api,
                "faturamento_bruto": business_metrics.get("faturamento_bruto", "N/A"),
                "unidades_vendidas": business_metrics.get("unidades_vendidas", "N/A"),
                "total_de_vendas": business_metrics.get("total_de_vendas", "N/A"),
                "ticket_medio": business_metrics.get("ticket_medio", "N/A"),
                "ads_cost": ads_metrics.get("cost", "N/A"),
                "ads_direct_amount": ads_metrics.get("direct_amount", "N/A"),
                "ads_indirect_amount": ads_metrics.get("indirect_amount", "N/A"),
                "ads_total_amount": ads_metrics.get("total_amount", "N/A"),
                "ads_acos": ads_metrics.get("acos", "N/A")
            }
            df_consolidated = pd.DataFrame([consolidated_data])
            update_keys_consolidated = ['periodo_consulta', 'cliente']
            export_to_google_sheets(df_consolidated, "Histórico de Vendas Meli - 2024", "Dados Consolidados", google_creds, update_key_cols=update_keys_consolidated)

            logger.info("Coletando dados detalhados de campanhas para o dia...")
            campaigns_data = collector.get_all_campaigns_paginated(advertiser_id, date_str)
            if campaigns_data:
                df_campaigns_raw = pd.json_normalize(campaigns_data)
                logger.info("Realizando analise estrategica...")
                df_analysis = analyze_and_consolidate(df_campaigns_raw)
                
                df_analysis.insert(0, 'data_geracao', timestamp_geracao)
                df_analysis.insert(1, 'periodo_consulta', date_str)
                df_analysis.insert(2, 'cliente', client_name_from_api)
                
                colunas_finais = [
                    'data_geracao', 'periodo_consulta', 'cliente', 'Nome_Campanha', 'status', 
                    'Orcamento_Campanha', 'Orcamento_Recomendado', 
                    'ACOS_Campanha', 'ACOS_Recomendado', 'Estrategia_Recomendada'
                ]
                # Garante que apenas colunas existentes sejam selecionadas
                colunas_existentes_df = [col for col in colunas_finais if col in df_analysis.columns]
                
                update_keys_campaigns = ['periodo_consulta', 'cliente', 'Nome_Campanha']
                export_to_google_sheets(df_analysis[colunas_existentes_df], "Histórico de Vendas Meli - 2024", "Analise de Campanhas", google_creds, update_key_cols=update_keys_campaigns)
            else:
                logger.info(f"Nenhuma campanha encontrada para {client_name_from_api} no dia de hoje.")

        except Exception as e:
            logger.error(f"ERRO INESPERADO ao processar o cliente {client_name}: {e}", exc_info=True)
            continue # Continua para o próximo cliente em caso de erro

    logger.info("Atualização em tempo real finalizada.")

if __name__ == "__main__":
    main()