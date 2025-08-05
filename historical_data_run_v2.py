import pandas as pd
import requests
import gspread
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging
from google.oauth2.service_account import Credentials
import os
from io import StringIO
import toml
import json

# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Nome do ficheiro de estado ---
STATE_FILE = "historical_run_v2_state.json"

# --- Funções de Estado e Autenticação ---
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def get_new_access_token(client_info):
    url = "https://api.mercadolibre.com/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "refresh_token", "client_id": client_info["app_id"], "client_secret": client_info["client_secret"], "refresh_token": client_info["refresh_token"]}
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao renovar o Access Token: {e.response.json() if e.response else e}")
        return None

# --- Módulo de Coleta Simplificado ---
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

    def get_business_metrics(self, seller_id, date_str):
        logger.info(f"Coletando métricas de negócio para {date_str}...")
        date_from_str, date_to_str = f"{date_str}T00:00:00.000-03:00", f"{date_str}T23:59:59.999-03:00"
        orders_metrics = {}
        try:
            params = {"seller": seller_id, "order.date_created.from": date_from_str, "order.date_created.to": date_to_str, "sort": "date_desc"}
            response = self.session.get(f"{self.base_url}/orders/search", params=params, timeout=self.timeout)
            response.raise_for_status()
            data = response.json()
            valid_orders = [o for o in data.get('results', []) if o.get('status') in ['paid', 'shipped', 'delivered']]
            if valid_orders:
                vendas_brutas = sum(o.get('total_amount', 0) for o in valid_orders)
                orders_metrics = {"faturamento_bruto": vendas_brutas, "unidades_vendidas": sum(item.get('quantity', 0) for o in valid_orders for item in o.get('order_items', [])), "quantidade_vendas": len(valid_orders)}
        except Exception as e:
            logger.error(f"Erro ao buscar pedidos: {e}")
        
        visits_metrics = {}
        try:
            url = f"https://api.mercadolibre.com/users/{seller_id}/items_visits?date_from={date_str}&date_to={date_str}"
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            visits_data = response.json()
            total_visits = visits_data.get("total_visits", 0)
            visits_metrics = {"visitas": total_visits}
        except Exception as e:
            logger.error(f"Erro ao buscar visitas: {e}")
        
        return {**orders_metrics, **visits_metrics}

    def get_ads_summary_metrics(self, advertiser_id, date_str):
        logger.info(f"Coletando métricas de publicidade para {date_str}...")
        try:
            params = {"date_from": date_str, "date_to": date_str, "metrics_summary": "true", "metrics": "cost,acos,direct_amount,indirect_amount,total_amount,clicks,prints"}
            response = self.session.get(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"}, timeout=self.timeout)
            response.raise_for_status()
            return response.json().get("metrics_summary", {})
        except Exception as e:
            logger.error(f"Erro ao obter resumo de publicidade: {e}")
            return {}

    def get_advertisers(self):
        try:
            response = self.session.get(f"{self.base_url}/advertising/advertisers", params={"product_id": "PADS"}, headers={"Api-Version": "1"}, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Erro ao obter anunciantes: {e}")
            return None

# --- FUNÇÃO DE EXPORTAÇÃO OTIMIZADA ---
def update_or_append_rows(df_new, worksheet, df_existing_cache, key_cols):
    logger.info(f"Iniciando atualização em lote da aba '{worksheet.title}' com {len(df_new)} novas linhas.")
    
    for col in key_cols:
        if col in df_existing_cache.columns: df_existing_cache[col] = df_existing_cache[col].astype(str)
        if col in df_new.columns: df_new[col] = df_new[col].astype(str)

    try:
        header = worksheet.row_values(1)
        if not header:
            header = df_new.columns.tolist()
            worksheet.update([header], value_input_option='USER_ENTERED')
    except gspread.exceptions.APIError as e:
        logger.error(f"ERRO DE API ao ler o cabeçalho de '{worksheet.title}'. Pausando por 60s. Erro: {e}")
        time.sleep(60); raise e

    updates_to_batch = []
    rows_to_append = []
    
    for _, new_row in df_new.iterrows():
        match_index = -1
        if not df_existing_cache.empty:
            condition = pd.Series([True] * len(df_existing_cache))
            for col in key_cols: condition &= (df_existing_cache[col] == new_row[col])
            matched_rows = df_existing_cache[condition]
            if not matched_rows.empty: match_index = matched_rows.index[0]
        
        if match_index != -1:
            row_to_update_num = match_index + 2
            existing_row_dict = df_existing_cache.iloc[match_index].to_dict()
            for col, value in new_row.items():
                if pd.notna(value) and str(value).strip() not in ["", "N/A"]: existing_row_dict[col] = value
            
            final_row_values = [existing_row_dict.get(col, "") for col in header]
            updates_to_batch.append({'range': f'A{row_to_update_num}', 'values': [final_row_values]})
            for col, value in new_row.items(): df_existing_cache.loc[match_index, col] = value
        else:
            df_aligned = pd.DataFrame(columns=header)
            df_aligned = pd.concat([df_aligned, pd.DataFrame([new_row])], ignore_index=True)
            rows_to_append.extend(df_aligned.fillna("").values.tolist())
            df_existing_cache = pd.concat([df_existing_cache, df_aligned], ignore_index=True)
            
    try:
        if updates_to_batch:
            worksheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
            logger.info(f"SUCESSO: {len(updates_to_batch)} linhas atualizadas em lote na aba '{worksheet.title}'.")
        if rows_to_append:
            worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            logger.info(f"SUCESSO: {len(rows_to_append)} novas linhas adicionadas em lote na aba '{worksheet.title}'.")
    except gspread.exceptions.APIError as e:
        logger.error(f"ERRO DE API ao escrever em lote em '{worksheet.title}'. Pausando por 60s. Erro: {e}")
        time.sleep(60); raise e

# --- FUNÇÃO MAIN SIMPLIFICADA ---
def main():
    logger.info("Iniciando a extração de dados históricos (v5 - Foco em Dados Consolidados).")
    
    try:
        if os.path.exists('.streamlit/secrets.toml'):
            secrets = toml.load('.streamlit/secrets.toml'); google_creds = secrets['google_credentials']
            with open('clients.csv', 'r') as f: clients_csv_data = f.read()
        else:
            google_creds_str = os.environ['GOOGLE_CREDENTIALS']; clients_csv_data = os.environ['MELI_CLIENTS_CSV']; google_creds = toml.loads(google_creds_str)['google_credentials']
        clients_df = pd.read_csv(StringIO(clients_csv_data))
    except Exception as e: logger.error(f"ERRO CRÍTICO ao carregar as credenciais: {e}"); return

    brasil_timezone = ZoneInfo("America/Sao_Paulo"); limit_date_past = datetime(2024, 1, 1, tzinfo=brasil_timezone); start_date_today = datetime.now(brasil_timezone); state = load_state()
    
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]; creds = Credentials.from_service_account_info(google_creds, scopes=scopes); client_gspread = gspread.authorize(creds); spreadsheet = client_gspread.open("Histórico de Vendas Meli - 2024")

    FINAL_COLUMNS_ORDER_CONSOLIDATED = ["data_geracao", "periodo_consulta", "cliente", "Faturamento", "Investimento", "Quantidade de Vendas", "Unidades Vendidas", "Visitas", "Taxa de Conversão Média", "ACOS", "TACOS", "ROAS", "ROI Média", "Vendas por Ads", "Vendas sem Ads", "Cliques", "CPC", "CTR", "Impressões"]

    for index, client_info in clients_df.iterrows():
        client_name = client_info["client_name"]; logger.info(f"\n{'='*50}\n--- Processando cliente: {client_name} ---\n{'='*50}")
        try:
            logger.info("Lendo dados existentes da planilha para o cache..."); 
            worksheet_consolidado = spreadsheet.worksheet("Dados Consolidados v2")
            df_consolidado_cache = pd.DataFrame(worksheet_consolidado.get_all_records())
            logger.info(f"Cache criado: {len(df_consolidado_cache)} linhas em Dados Consolidados.")
            
            last_processed_date_str = state.get(client_name)
            if last_processed_date_str: start_date_for_run = datetime.strptime(last_processed_date_str, '%Y-%m-%d').replace(tzinfo=brasil_timezone) - timedelta(days=1)
            else: start_date_for_run = start_date_today
            if start_date_for_run.date() < limit_date_past.date(): logger.info(f"Cliente já está atualizado. Pulando."); continue

            date_range = pd.date_range(start=limit_date_past, end=start_date_for_run, tz=brasil_timezone); reversed_date_range = sorted(date_range, reverse=True)
            access_token = get_new_access_token(client_info)
            if not access_token: continue
            collector = MercadoLivreAdsCollector(access_token); advertisers_data = collector.get_advertisers()
            if not advertisers_data or not advertisers_data.get('advertisers'): continue
            advertiser = advertisers_data['advertisers'][0]; advertiser_id = advertiser['advertiser_id']; client_name_from_api = advertiser.get('advertiser_name', client_name); user_id = collector.get_user_id()
            if not user_id: continue

            for single_date in reversed_date_range:
                date_str = single_date.strftime('%Y-%m-%d'); logger.info(f"--- Processando dia: {date_str} ---")
                timestamp_geracao = datetime.now(brasil_timezone).strftime('%Y-%m-%d %H:%M:%S')
                business_metrics = collector.get_business_metrics(user_id, date_str)
                ads_metrics = collector.get_ads_summary_metrics(advertiser_id, date_str)
                
                faturamento = pd.to_numeric(business_metrics.get("faturamento_bruto"), errors='coerce'); qtde_vendas = pd.to_numeric(business_metrics.get("quantidade_vendas"), errors='coerce'); visitas = pd.to_numeric(business_metrics.get("visitas"), errors='coerce'); unidades_vendidas = pd.to_numeric(business_metrics.get("unidades_vendidas"), errors='coerce'); investimento_ads = pd.to_numeric(ads_metrics.get("cost"), errors='coerce'); vendas_ads = pd.to_numeric(ads_metrics.get("total_amount"), errors='coerce'); impressoes = pd.to_numeric(ads_metrics.get("prints"), errors='coerce'); cliques = pd.to_numeric(ads_metrics.get("clicks"), errors='coerce'); acos = pd.to_numeric(ads_metrics.get("acos"), errors='coerce')
                taxa_conversao_media = (qtde_vendas / visitas * 100) if pd.notna(qtde_vendas) and pd.notna(visitas) and visitas > 0 else 0; tacos = (investimento_ads / faturamento * 100) if pd.notna(investimento_ads) and pd.notna(faturamento) and faturamento > 0 else 0; roas = (vendas_ads / investimento_ads) if pd.notna(vendas_ads) and pd.notna(investimento_ads) and investimento_ads > 0 else 0; vendas_sem_ads = (faturamento - vendas_ads) if pd.notna(faturamento) and pd.notna(vendas_ads) else faturamento; cpc = (investimento_ads / cliques) if pd.notna(investimento_ads) and pd.notna(cliques) and cliques > 0 else 0; ctr = (cliques / impressoes * 100) if pd.notna(cliques) and pd.notna(impressoes) and impressoes > 0 else 0
                final_data = { "data_geracao": timestamp_geracao, "periodo_consulta": date_str, "cliente": client_name_from_api };
                if pd.notna(faturamento): final_data["Faturamento"] = f"R$ {faturamento:,.2f}"
                if pd.notna(investimento_ads): final_data["Investimento"] = f"R$ {investimento_ads:,.2f}"
                if pd.notna(qtde_vendas): final_data["Quantidade de Vendas"] = int(qtde_vendas)
                if pd.notna(unidades_vendidas): final_data["Unidades Vendidas"] = int(unidades_vendidas)
                if pd.notna(visitas): final_data["Visitas"] = int(visitas)
                if taxa_conversao_media > 0: final_data["Taxa de Conversão Média"] = f"{taxa_conversao_media:.2f}%"
                if pd.notna(acos): final_data["ACOS"] = f"{acos:.2f}%"
                if tacos > 0: final_data["TACOS"] = f"{tacos:.2f}%"
                if roas > 0: final_data["ROAS"] = f"{roas:.2f}"; final_data["ROI Média"] = f"{roas:.2f}"
                if pd.notna(vendas_ads): final_data["Vendas por Ads"] = f"R$ {vendas_ads:,.2f}"
                if pd.notna(vendas_sem_ads): final_data["Vendas sem Ads"] = f"R$ {vendas_sem_ads:,.2f}"
                if pd.notna(cliques): final_data["Cliques"] = int(cliques)
                if cpc > 0: final_data["CPC"] = f"R$ {cpc:,.2f}"
                if ctr > 0: final_data["CTR"] = f"{ctr:.2f}%"
                if pd.notna(impressoes): final_data["Impressões"] = int(impressoes)
                
                df_final_consolidated = pd.DataFrame([final_data]).reindex(columns=FINAL_COLUMNS_ORDER_CONSOLIDATED)
                update_keys_consolidated = ['periodo_consulta', 'cliente']
                update_or_append_rows(df_final_consolidated, worksheet_consolidado, df_consolidado_cache, update_keys_consolidated)

                state[client_name] = date_str
                save_state(state)
                time.sleep(1) 
        except gspread.exceptions.APIError as e: logger.error(f"ERRO DE API GRAVE ao processar {client_name}. A quota pode ter sido excedida. O script continuará para o próximo cliente. Erro: {e}", exc_info=True); continue
        except Exception as e: failed_date_str = locals().get('date_str', 'antes do loop de datas'); logger.error(f"ERRO INESPERADO ao processar {client_name} no dia {failed_date_str}. Erro: {e}", exc_info=True); continue
    logger.info("\nExecução da extração histórica (v5) finalizada.")

if __name__ == "__main__":
    main()