import requests
import pandas as pd
import os
from urllib.parse import urlparse, parse_qs

CLIENTS_FILE = "clients.csv"
REDIRECT_URL = "https://oauth.pstmn.io/v1/callback"

def get_initial_tokens(auth_code, app_id, client_secret):
    """Troca o código de autorização por tokens usando as credenciais do cliente."""
    url = "https://api.mercadolibre.com/oauth/token"
    data = {
        "grant_type": "authorization_code",
        "client_id": app_id,
        "client_secret": client_secret,
        "code": auth_code,
        "redirect_uri": REDIRECT_URL
    }
    response = requests.post(url, headers={"Content-Type": "application/x-www-form-urlencoded"}, data=data)
    response.raise_for_status()
    return response.json()

def get_advertiser_info(access_token):
    """Busca informações do anunciante."""
    url = f"https://api.mercadolibre.com/advertising/advertisers?product_id=PADS"
    response = requests.get(url, headers={"Authorization": f"Bearer {access_token}", "Api-Version": "1"})
    response.raise_for_status()
    data = response.json()
    if data and data.get("advertisers"):
        return data["advertisers"][0].get("advertiser_name")
    return None

def main():
    """Função principal para cadastrar um novo cliente."""
    print("-" * 70)
    print("Iniciando o cadastro de um novo cliente...")
    
    app_id = input("PASSO 1: Insira o APP_ID (Client ID) do cliente:\n> ")
    client_secret = input("PASSO 2: Insira a CLIENT_SECRET do cliente:\n> ")

    if not app_id or not client_secret:
        print("ERRO: APP_ID e CLIENT_SECRET são obrigatórios.")
        return

    auth_url = f"https://auth.mercadolivre.com.br/authorization?response_type=code&client_id={app_id}&redirect_uri={REDIRECT_URL}"
    print("-" * 70 + f"\nPASSO 3: Peça ao cliente para acessar a URL abaixo, autorizar e te enviar a URL final:\n{auth_url}\n" + "-" * 70)
    
    redirect_url_with_code = input("PASSO 4: Cole a URL COMPLETA de redirecionamento aqui:\n> ")
    
    try:
        auth_code = parse_qs(urlparse(redirect_url_with_code).query).get("code", [None])[0]
        if not auth_code: raise ValueError("Código não encontrado na URL.")
        
        print(f"\nCódigo extraído: {auth_code}\nTrocando por tokens...")
        token_data = get_initial_tokens(auth_code, app_id, client_secret)
        access_token, refresh_token = token_data["access_token"], token_data["refresh_token"]
        
        print("Buscando nome do anunciante...")
        client_name = get_advertiser_info(access_token)
        if not client_name: raise ValueError("Não foi possível obter o nome do anunciante.")
        
        new_client_data = {"client_name": [client_name], "app_id": [app_id], "client_secret": [client_secret], "refresh_token": [refresh_token]}
        df_new = pd.DataFrame(new_client_data)

        if os.path.exists(CLIENTS_FILE):
            df_existing = pd.read_csv(CLIENTS_FILE)
            if client_name in df_existing['client_name'].values:
                print(f"AVISO: O cliente '{client_name}' já existe. Atualizando suas credenciais.")
                df_existing = df_existing[df_existing['client_name'] != client_name]
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_final = df_new
            
        df_final.to_csv(CLIENTS_FILE, index=False)
        
        print("\n" + "="*70 + f"\nSUCESSO! Cliente '{client_name}' cadastrado em '{CLIENTS_FILE}'.\n" + "="*70)

    except Exception as e:
        print(f"\nERRO: {e}")

if __name__ == "__main__":
    main()