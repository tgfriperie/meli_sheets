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

# --- Configuração ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constantes do Script ---
TARGET_SPREADSHEET_NAME = "Histórico de Vendas Meli - 2024" # Verifique se este é o nome exato da sua planilha
TARGET_WORKSHEET_NAME = "Dados_Horarios"
STATE_FILE = "hourly_run_state.json" # Arquivo de estado para este script

# --- Funções Reutilizadas (Baseadas no daily_collector.py) ---

def get_new_access_token(client_info):
    """Renova o access token do Mercado Livre."""
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
        logger.error(f"Erro ao renovar o Access Token: {e.response.json() if e.response else e}")
        return None

def export_to_gsheets_append_only(df, worksheet_name, google_creds):
    """Função simplificada para apenas adicionar novas linhas a uma aba."""
    if df.empty:
        logger.info("Nenhum dado novo para exportar.")
        return

    logger.info(f"Exportando {len(df)} linhas para a aba '{worksheet_name}'...")
    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(google_creds, scopes=scopes)
        client = gspread.authorize(creds)
        spreadsheet = client.open(TARGET_SPREADSHEET_NAME)
        
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows="1", cols=len(df.columns))
            worksheet.update([df.columns.tolist()], value_input_option='USER_ENTERED')
            logger.info(f"Aba '{worksheet_name}' criada com sucesso.")
        
        if not worksheet.row_values(1): # Garante que o cabeçalho exista
             worksheet.update([df.columns.tolist()], value_input_option='USER_ENTERED')

        worksheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
        logger.info(f"SUCESSO: {len(df)} linhas adicionadas à aba '{worksheet_name}'.")

    except Exception as e:
        logger.error(f"ERRO AO EXPORTAR PARA '{worksheet_name}': {e}", exc_info=True)

# --- Funções de Lógica Principal ---

def load_state():
    """Carrega o último dia processado de um arquivo de estado."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    """Salva o estado atual da execução."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def get_all_orders_for_day(access_token, seller_id, date_str):
    """Busca todos os pedidos de um dia específico, lidando com paginação."""
    all_orders = []
    offset = 0
    limit = 50
    headers = {"Authorization": f"Bearer {access_token}"}
    date_from = f"{date_str}T00:00:00.000-03:00"
    date_to = f"{date_str}T23:59:59.999-03:00"

    logger.info(f"Buscando pedidos para o dia {date_str}...")
    while True:
        params = {"seller": seller_id, "order.date_created.from": date_from, "order.date_created.to": date_to, "sort": "date_asc", "limit": limit, "offset": offset}
        try:
            response = requests.get("https://api.mercadolibre.com/orders/search", params=params, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            if not results: break
            all_orders.extend(results)
            offset += limit
            if offset >= data.get('paging', {}).get('total', 0): break
            time.sleep(0.3)
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na API do Meli ao buscar pedidos: {e}"); break
            
    logger.info(f"Encontrados {len(all_orders)} pedidos para {date_str}.")
    return all_orders

def main():
    logger.info(f"Iniciando script de exportação de dados horários.")
    
    # Carregamento de credenciais e clientes
    try:
        secrets = toml.load(".streamlit/secrets.toml")
        google_creds = secrets['connections']['gcs']['service_account_info']
        clients_df = pd.read_csv("clients.csv")
    except Exception as e:
        logger.error(f"ERRO CRÍTICO: Não foi possível carregar 'secrets.toml' ou 'clients.csv'. Verifique os arquivos. Erro: {e}")
        return

    state = load_state()
    brasil_timezone = ZoneInfo("America/Sao_Paulo")
    limit_date_past = datetime(2024, 1, 1, tzinfo=brasil_timezone)

    for _, client_row in clients_df.iterrows():
        client_name = client_row["client_name"]
        logger.info(f"\n--- Processando cliente: {client_name} ---")

        access_token = get_new_access_token(client_row)
        if not access_token: continue

        try:
            response_user = requests.get("https://api.mercadolibre.com/users/me", headers={"Authorization": f"Bearer {access_token}"})
            response_user.raise_for_status()
            seller_id = response_user.json().get('id')
            if not seller_id: logger.error(f"Não foi possível obter seller_id para {client_name}."); continue
        except Exception as e:
            logger.error(f"Erro ao buscar seller_id para {client_name}: {e}"); continue
        
        last_processed_date_str = state.get(client_name)
        start_date = datetime.now(brasil_timezone) if not last_processed_date_str else datetime.strptime(last_processed_date_str, '%Y-%m-%d').replace(tzinfo=brasil_timezone) - timedelta(days=1)
        
        if start_date.date() < limit_date_past.date():
            logger.info(f"Cliente {client_name} já possui todo o histórico. Pulando."); continue
            
        date_range = pd.date_range(start=limit_date_past, end=start_date, tz=brasil_timezone)
        
        for single_date in sorted(date_range, reverse=True):
            date_str = single_date.strftime('%Y-%m-%d')
            logger.info(f"Processando data: {date_str}")
            
            orders = get_all_orders_for_day(access_token, seller_id, date_str)
            if not orders:
                state[client_name] = date_str
                save_state(state)
                continue

            hourly_rows = []
            for order in orders:
                if order.get('status') not in ['paid', 'shipped', 'delivered']: continue
                
                dt_object = pd.to_datetime(order['date_created'])
                
                row_data = {
                    "data_hora": dt_object.strftime('%Y-%m-%d %H:%M:%S'),
                    "quantidade_vendas": sum(item.get('quantity', 0) for item in order.get('order_items', [])),
                    "faturamento": order.get('total_amount', 0),
                    "cliente": client_name
                }
                hourly_rows.append(row_data)

            if hourly_rows:
                df_to_export = pd.DataFrame(hourly_rows)
                export_to_gsheets_append_only(df_to_export, TARGET_WORKSHEET_NAME, google_creds)

            state[client_name] = date_str
            save_state(state)
            logger.info(f"Progresso para {client_name} salvo. Último dia processado: {date_str}")
            time.sleep(1) # Pausa entre os dias

    logger.info("\nExecução finalizada.")

if __name__ == "__main__":
    main()