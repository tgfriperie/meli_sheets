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

# --- Constantes e Configurações ---
STATE_FILE = "historical_run_v15_state.json"
API_TIMEOUT = 60
MAX_RETRIES = 3

# --- Funções de Estado e Autenticação ---
def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def get_new_access_token(client_info):
    url = "https://api.mercadolibre.com/oauth/token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {
        "grant_type": "refresh_token", "client_id": client_info["app_id"],
        "client_secret": client_info["client_secret"], "refresh_token": client_info["refresh_token"]
    }
    try:
        response = requests.post(url, headers=headers, data=data, timeout=API_TIMEOUT)
        response.raise_for_status()
        logger.info("Access Token renovado com sucesso.")
        return response.json()["access_token"]
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao renovar o Access Token: {e.response.json() if e.response else e}")
        return None

# --- Módulo de Coleta de Dados ---
class MercadoLivreAdsCollector:
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://api.mercadolibre.com"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.access_token}"})

    def _make_request(self, url, params=None, headers=None):
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=API_TIMEOUT)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Tentativa {attempt + 1}/{MAX_RETRIES} falhou para {url}. Erro: {e}")
                if attempt + 1 == MAX_RETRIES:
                    logger.error("Número máximo de retentativas atingido.")
                    raise
                time.sleep(5 * (attempt + 1))
        return None

    def get_user_id(self):
        try:
            data = self._make_request(f"{self.base_url}/users/me")
            return data.get('id') if data else None
        except Exception as e:
            logger.error(f"Não foi possível obter o ID do usuário: {e}")
            return None

    def get_business_metrics(self, seller_id, date_str):
        logger.info(f"Iniciando coleta de métricas para {date_str}...")
        brasil_timezone = ZoneInfo("America/Sao_Paulo")
        target_date_object = datetime.strptime(date_str, '%Y-%m-%d').date()
        date_from_str = f"{date_str}T00:00:00.000-03:00"
        date_to_str = f"{date_str}T23:59:59.999-03:00"
        
        all_orders, offset, limit = [], 0, 50
        
        while True:
            params = {
                "seller": seller_id, "order.date_created.from": date_from_str,
                "order.date_created.to": date_to_str, "sort": "date_desc",
                "offset": offset, "limit": limit
            }
            logger.info(f"Buscando pedidos... Página com offset {offset}")
            data = self._make_request(f"{self.base_url}/orders/search", params=params)
            if not data:
                raise Exception(f"Falha irrecuperável ao buscar página de pedidos com offset {offset}")
            page_orders = data.get('results', [])
            all_orders.extend(page_orders)
            
            paging = data.get('paging', {})
            if (paging.get('offset', 0) + limit) >= paging.get('total', 0):
                logger.info(f"Paginação concluída. Total de {len(all_orders)} pedidos recebidos da API.")
                break
            offset += limit

        valid_orders = []
        reasons_for_discard = {'wrong_date': 0, 'test_order': 0}

        for order in all_orders:
            order_date_obj = pd.to_datetime(order.get("date_created")).tz_convert(brasil_timezone)
            if order_date_obj.date() != target_date_object:
                reasons_for_discard['wrong_date'] += 1; continue
            
            if "test_order" in order.get("tags", []):
                reasons_for_discard['test_order'] += 1; continue
            
            # ===============================================================================
            # ALTERAÇÃO FINAL: Lógica de Espelhamento do Painel.
            # Nenhum filtro de status é aplicado. Todos os pedidos (exceto de teste)
            # que ocorreram na data correta são contados.
            # ===============================================================================
            valid_orders.append(order)
        
        logger.info(f"Pedidos válidos para soma (após filtro de data e teste): {len(valid_orders)}.")
        logger.info(f"Pedidos descartados: {reasons_for_discard}")

        orders_metrics = {}
        if valid_orders:
            faturamento = sum(o.get('total_amount', 0) for o in valid_orders)
            unidades_vendidas = sum(item.get('quantity', 0) for o in valid_orders for item in o.get('order_items', []))
            orders_metrics = {"faturamento_bruto": faturamento, "unidades_vendidas": unidades_vendidas, "quantidade_vendas": len(valid_orders)}
        
        logger.info(f"-- Resumo do dia {date_str} -- Vendas: {orders_metrics.get('quantidade_vendas', 0)}, Unidades: {orders_metrics.get('unidades_vendidas', 0)}, Faturamento: R$ {orders_metrics.get('faturamento_bruto', 0):.2f}")
        
        visits_metrics = {}
        try:
            visits_data = self._make_request(f"https://api.mercadolibre.com/users/{seller_id}/items_visits", params={"date_from": date_str, "date_to": date_str})
            if visits_data: visits_metrics = {"visitas": visits_data.get("total_visits", 0)}
        except Exception as e:
            logger.error(f"Falha ao buscar visitas para o dia {date_str}: {e}")

        return {**orders_metrics, **visits_metrics}

    def get_ads_summary_metrics(self, advertiser_id, date_str):
        params = {"date_from": date_str, "date_to": date_str, "metrics_summary": "true", "metrics": "cost,acos,direct_amount,indirect_amount,total_amount,clicks,prints"}
        try:
            data = self._make_request(f"{self.base_url}/advertising/advertisers/{advertiser_id}/product_ads/campaigns", params=params, headers={"Api-Version": "2"})
            return data.get("metrics_summary", {}) if data else {}
        except Exception as e:
            logger.error(f"Falha ao buscar métricas de publicidade para {date_str}: {e}")
            return {}
        
    def get_advertisers(self):
        try:
            data = self._make_request(f"{self.base_url}/advertising/advertisers", params={"product_id": "PADS"}, headers={"Api-Version": "1"})
            return data if data else None
        except Exception as e:
            logger.error(f"Falha ao buscar anunciantes: {e}")
            return None

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
    updates_to_batch, rows_to_append = [], []
    for _, new_row in df_new.iterrows():
        match_index = -1
        if not df_existing_cache.empty:
            condition = pd.Series([True] * len(df_existing_cache))
            for col in key_cols:
                if col in df_existing_cache: condition &= (df_existing_cache[col] == new_row[col])
            matched_rows = df_existing_cache[condition]
            if not matched_rows.empty: match_index = matched_rows.index[0]
        if match_index != -1:
            row_to_update_num = match_index + 2
            existing_row_dict = df_existing_cache.iloc[match_index].to_dict()
            for col, value in new_row.items():
                if pd.notna(value) and str(value).strip() not in ["", "N/A"]: existing_row_dict[col] = value
            final_row_values = [existing_row_dict.get(col, "") for col in header]
            updates_to_batch.append({'range': f'A{row_to_update_num}', 'values': [final_row_values]})
            for col, value in new_row.items():
                if col in df_existing_cache: df_existing_cache.loc[match_index, col] = value
        else:
            df_aligned = pd.DataFrame([new_row]).reindex(columns=header)
            rows_to_append.extend(df_aligned.fillna("").values.tolist())
            df_existing_cache = pd.concat([df_existing_cache, df_aligned], ignore_index=True)
    try:
        if updates_to_batch:
            worksheet.batch_update(updates_to_batch, value_input_option='USER_ENTERED')
            logger.info(f"SUCESSO: {len(updates_to_batch)} linhas atualizadas em lote.")
        if rows_to_append:
            worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
            logger.info(f"SUCESSO: {len(rows_to_append)} novas linhas adicionadas.")
    except gspread.exceptions.APIError as e:
        logger.error(f"ERRO DE API ao escrever em lote. Pausando por 60s. Erro: {e}")
        time.sleep(60); raise e


def main():
    logger.info("Iniciando a extração de dados históricos (v15 - Espelhamento Total do Painel).")
    
    try:
        if os.path.exists('.streamlit/secrets.toml'):
            secrets = toml.load('.streamlit/secrets.toml'); google_creds = secrets['google_credentials']
            with open('clients.csv', 'r') as f: clients_csv_data = f.read()
        else:
            google_creds_str = os.environ['GOOGLE_CREDENTIALS']; clients_csv_data = os.environ['MELI_CLIENTS_CSV']; google_creds = toml.loads(google_creds_str)['google_credentials']
        clients_df = pd.read_csv(StringIO(clients_csv_data))
        clients_df['client_name'] = clients_df['client_name'].str.strip()
    except Exception as e:
        logger.critical(f"ERRO CRÍTICO ao carregar as credenciais ou o arquivo CSV: {e}")
        return

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(google_creds, scopes=scopes)
        client_gspread = gspread.authorize(creds)
        spreadsheet = client_gspread.open("Histórico de Vendas Meli - 2024")
        worksheet_consolidado = spreadsheet.worksheet("Dados Consolidados v2")
        df_consolidado_cache = pd.DataFrame(worksheet_consolidado.get_all_records())
    except Exception as e:
        logger.critical(f"ERRO CRÍTICO ao conectar-se com o Google Sheets: {e}")
        return

    state = load_state()
    brasil_timezone = ZoneInfo("America/Sao_Paulo")
    
    for _, client_info in clients_df.iterrows():
        client_name = client_info["client_name"]
        logger.info(f"\n{'='*50}\n--- Processando cliente: {client_name} ---\n{'='*50}")

        default_limit_date_past = datetime(2024, 1, 1, tzinfo=brasil_timezone)
        limit_date_past = default_limit_date_past
        if 'start_date' in client_info and pd.notna(client_info['start_date']):
            try:
                client_specific_start_date = datetime.strptime(str(client_info['start_date']), '%Y-%m-%d').replace(tzinfo=brasil_timezone)
                if client_specific_start_date > limit_date_past: limit_date_past = client_specific_start_date
            except ValueError: logger.warning(f"Formato de data inválido para '{client_name}'. Usando padrão.")
        
        last_processed_date_str = state.get(client_name)
        start_date_for_run = (datetime.strptime(last_processed_date_str, '%Y-%m-%d').replace(tzinfo=brasil_timezone) - timedelta(days=1)) if last_processed_date_str else datetime.now(brasil_timezone)

        if start_date_for_run.date() < limit_date_past.date():
            logger.info(f"Cliente '{client_name}' já está atualizado até sua data de início. Pulando.")
            continue

        date_range = pd.date_range(start=limit_date_past, end=start_date_for_run, tz=brasil_timezone)
        logger.info(f"Período a ser processado para '{client_name}': de {date_range.min().date()} até {date_range.max().date()}.")
        
        access_token = get_new_access_token(client_info)
        if not access_token: continue

        collector = MercadoLivreAdsCollector(access_token)
        
        user_id = collector.get_user_id()
        if not user_id:
            logger.error(f"Não foi possível obter o user_id para {client_name}. Pulando para o próximo cliente.")
            continue

        advertisers_data = collector.get_advertisers()
        advertiser_id, client_name_from_api = None, client_name
        if advertisers_data and advertisers_data.get('advertisers'):
            advertiser = advertisers_data['advertisers'][0]
            advertiser_id = advertiser['advertiser_id']
            client_name_from_api = advertiser.get('advertiser_name', client_name)
        else:
            logger.warning(f"Nenhum anunciante encontrado para {client_name}. Métricas de Ads não serão coletadas.")
        
        for single_date in sorted(date_range, reverse=True):
            date_str = single_date.strftime('%Y-%m-%d')
            
            try:
                business_metrics = collector.get_business_metrics(seller_id=user_id, date_str=date_str)
                ads_metrics = collector.get_ads_summary_metrics(advertiser_id, date_str) if advertiser_id else {}
                
                faturamento = pd.to_numeric(business_metrics.get("faturamento_bruto"), errors='coerce')
                qtde_vendas = pd.to_numeric(business_metrics.get("quantidade_vendas"), errors='coerce')
                visitas = pd.to_numeric(business_metrics.get("visitas"), errors='coerce')
                unidades_vendidas = pd.to_numeric(business_metrics.get("unidades_vendidas"), errors='coerce')
                investimento_ads = pd.to_numeric(ads_metrics.get("cost"), errors='coerce')
                vendas_ads = pd.to_numeric(ads_metrics.get("total_amount"), errors='coerce')
                impressoes = pd.to_numeric(ads_metrics.get("prints"), errors='coerce')
                cliques = pd.to_numeric(ads_metrics.get("clicks"), errors='coerce')
                acos_percent = pd.to_numeric(ads_metrics.get("acos"), errors='coerce')

                taxa_conversao = (qtde_vendas / visitas * 100) if pd.notna(qtde_vendas) and pd.notna(visitas) and visitas > 0 else 0
                tacos = (investimento_ads / faturamento * 100) if pd.notna(investimento_ads) and pd.notna(faturamento) and faturamento > 0 else 0
                roas = (vendas_ads / investimento_ads) if pd.notna(vendas_ads) and pd.notna(investimento_ads) and investimento_ads > 0 else 0
                vendas_sem_ads = (faturamento - vendas_ads) if pd.notna(faturamento) and pd.notna(vendas_ads) else faturamento
                cpc = (investimento_ads / cliques) if pd.notna(investimento_ads) and pd.notna(cliques) and cliques > 0 else 0
                ctr = (cliques / impressoes * 100) if pd.notna(cliques) and pd.notna(impressoes) and impressoes > 0 else 0

                final_data = {
                    "data_geracao": datetime.now(brasil_timezone).strftime('%Y-%m-%d %H:%M:%S'),
                    "periodo_consulta": date_str,
                    "cliente": client_name_from_api,
                    "Faturamento": f"R$ {faturamento:,.2f}" if pd.notna(faturamento) else "R$ 0,00",
                    "Investimento": f"R$ {investimento_ads:,.2f}" if pd.notna(investimento_ads) else None,
                    "Quantidade de Vendas": int(qtde_vendas) if pd.notna(qtde_vendas) else 0,
                    "Unidades Vendidas": int(unidades_vendidas) if pd.notna(unidades_vendidas) else 0,
                    "Visitas": int(visitas) if pd.notna(visitas) else 0,
                    "Taxa de Conversão Média": f"{taxa_conversao:.2f}%" if taxa_conversao > 0 else None,
                    "ACOS": f"{acos_percent:.2f}%" if pd.notna(acos_percent) else None,
                    "TACOS": f"{tacos:.2f}%" if tacos > 0 else None,
                    "ROAS": f"{roas:.2f}" if roas > 0 else None,
                    "ROI Média": f"{roas:.2f}" if roas > 0 else None,
                    "Vendas por Ads": f"R$ {vendas_ads:,.2f}" if pd.notna(vendas_ads) else None,
                    "Vendas sem Ads": f"R$ {vendas_sem_ads:,.2f}" if pd.notna(vendas_sem_ads) else None,
                    "Cliques": int(cliques) if pd.notna(cliques) else None,
                    "CPC": f"R$ {cpc:,.2f}" if cpc > 0 else None,
                    "CTR": f"{ctr:.2f}%" if ctr > 0 else None,
                    "Impressões": int(impressoes) if pd.notna(impressoes) else None,
                }

                FINAL_COLUMNS_ORDER = list(df_consolidado_cache.columns)
                if not FINAL_COLUMNS_ORDER:
                    FINAL_COLUMNS_ORDER = list(final_data.keys())

                df_final = pd.DataFrame([final_data]).reindex(columns=FINAL_COLUMNS_ORDER)
                update_or_append_rows(df_final, worksheet_consolidado, df_consolidado_cache, ['periodo_consulta', 'cliente'])

                state[client_name] = date_str
                save_state(state)
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"ERRO IRRECUPERÁVEL ao processar o dia {date_str} para {client_name}. O script continuará para o próximo cliente. Erro: {e}", exc_info=True)
                break 

    logger.info("\nExecução da extração histórica (v15) finalizada.")

if __name__ == "__main__":
    main()
