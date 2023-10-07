#%%

import json
import pandas as pd
from google.cloud import bigquery
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import requests
from google.oauth2 import service_account
import pandas_gbq
from google.auth import credentials
import platform

if platform.system() == 'Linux':
    credentials = service_account.Credentials.from_service_account_file(os.path.dirname(os.path.realpath(__file__))+f'/acoes-*******.json')
    print("Sistema operacional: Linux")
elif platform.system() == 'Windows':
    # Código a ser executado se o sistema for Windows
    credentials = service_account.Credentials.from_service_account_file(
        os.path.dirname(os.path.realpath(__file__))+f'\\acoes-*******.json')

text_email = ''

def sendEmail():
    global text_email

    if 'ERRO' in text_email:
        titulo = 'ERRO Rotina Ibovespa'
    else:
        titulo = 'SUCESSO Rotina Ibovespa'
    

    me = 'mikeekrll@gmail.com'  # E-mail de envio
    you = ['mike-william98@hotmail.com','mwdoop98@gmail.com']  # E-mail de recebimento
    msg = MIMEMultipart('alternative')
    msg['Subject'] = titulo
    msg['From'] = 'mikeekrll@gmail.com'
    # E-mail de recebimento
    msg['To'] = 'mike-william98@hotmail.com, mwdoop98@gmail.com'
    text = text_email
    html = f"""\
        <html>
        <head></head>
        <body>
            <font face="Courier New, Courier, monospace">{text_email}<br></font>
        </body>
        </html>
        """
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
    msg.attach(part1)
    msg.attach(part2)
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login('mikeekrll@gmail.com', '*******')

    for email in you:
        mail.sendmail(me, email, msg.as_string())
    mail.quit()

def print_msg(msg):
    print(msg)
    global text_email
    text_email = f'{text_email}{msg}\n'


def upload_df_bq(df, nome_schema, nome_tabela, table_schema, orientacion):
    global credentials
    pandas_gbq.to_gbq(
        dataframe=df,
        destination_table=f'{nome_schema}.{nome_tabela}',
        project_id='acoes-378306',
        credentials=credentials,
        if_exists=orientacion,  # replace, append
        table_schema=table_schema,
    )

def main():
    try:
        print_msg('Processo dados historicos:')
        #%%
        #Full data ibovespa
        # url = 'https://brapi.dev/api/quote/%5EBVSP?range=40y&interval=1d&fundamental=true&dividends=false'

        #last 5 days
        url = 'https://brapi.dev/api/quote/%5EBVSP?range=5d&interval=1d&fundamental=true&dividends=false'
        response = requests.get(url)
        dados = json.loads(response.text)
        dados_history = dados["results"][0]['historicalDataPrice']
        # print_msg(dados)

        dados_hitory_df = pd.DataFrame(dados_history)
        #Transforma dados da coluna de data no padrao unix para data normal
        dados_hitory_df['date'] = pd.to_datetime(dados_hitory_df['date'], unit='s')
        # dados_df.to_csv(os.path.dirname(os.path.realpath(__file__)) +f'\\ibovepa_historico.csv', sep=';', index=False)
        print_msg('Dados extraidos da API!')

        str_to_delete = ''
        for date_time in dados_hitory_df['date']:
            if date_time == dados_hitory_df['date'][0]:
                str_to_delete = f"'{str(date_time)[:10]}'"
            else: 
                str_to_delete = f"{str_to_delete}, '{str(date_time)[:10]}'"

        delete = f'delete from `acoes-378306.acoes.ibovespa_historico` as i where cast(i.date as DATE) in ({str_to_delete})'
        print_msg(delete)

        client=bigquery.Client(credentials = credentials, project = 'acoes-378306')
        query_job = client.query(delete)
        print_msg('Dados apagados do big query!')
        # %%
        nome_schema = 'acoes'
        nome_tabela = 'ibovespa_historico'
        table_schema = [
            {'name': 'date', 'type': 'TIMESTAMP'},
            {'name': 'open', 'type': 'FLOAT'},
            {'name': 'high', 'type': 'FLOAT'},
            {'name': 'low', 'type': 'FLOAT'},
            {'name': 'close', 'type': 'FLOAT'},
            {'name': 'volume', 'type': 'FLOAT'},
            {'name': 'adjustedClose', 'type': 'FLOAT'},

        ]
        orientacion = 'append'
        upload_df_bq(dados_hitory_df, nome_schema, nome_tabela, table_schema, orientacion)


        print_msg('Dados atualizados uplodados com SUCESSO!')
        print_msg('-'*30)
        #%%
        print_msg('Processo dados cabecalho:')
        dados_cabecalho = str(dados["results"][0]).replace("'", '"').split('"validRanges"')[0][:-2]+'}'
        json_cabecalho = json.loads(dados_cabecalho)
        df_cabecalho = pd.DataFrame([json_cabecalho])

        # Lista das colunas na ordem desejada
        colunas_desejadas = ['symbol', 'shortName', 'longName', 'currency', 'regularMarketPrice',
                            'regularMarketDayHigh', 'regularMarketDayLow', 'regularMarketDayRange',
                            'regularMarketChange', 'regularMarketChangePercent', 'regularMarketTime',
                            'regularMarketVolume', 'regularMarketPreviousClose', 'regularMarketOpen',
                            'averageDailyVolume10Day', 'averageDailyVolume3Month', 'fiftyTwoWeekLowChange',
                            'fiftyTwoWeekRange', 'fiftyTwoWeekHighChange', 'fiftyTwoWeekHighChangePercent',
                            'fiftyTwoWeekLow', 'fiftyTwoWeekHigh', 'twoHundredDayAverage',
                            'twoHundredDayAverageChange', 'twoHundredDayAverageChangePercent']

        # Reordenar colunas e remover colunas extras
        df_cabecalho = df_cabecalho.reindex(columns=colunas_desejadas)

        # Remover colunas extras (caso existam)
        colunas_extras = set(df_cabecalho.columns) - set(colunas_desejadas)
        df_cabecalho = df_cabecalho.drop(columns=colunas_extras)
        # print(df_cabecalho.dtypes)

        nome_schema = 'acoes'
        nome_tabela = 'ibovespa_cabecalho'
        data_structure = [
            {'name': 'symbol', 'type': 'STRING'},
            {'name': 'shortName', 'type': 'STRING'},
            {'name': 'longName', 'type': 'STRING'},
            {'name': 'currency', 'type': 'STRING'},
            {'name': 'regularMarketPrice', 'type': 'FLOAT'},
            {'name': 'regularMarketDayHigh', 'type': 'FLOAT'},
            {'name': 'regularMarketDayLow', 'type': 'FLOAT'},
            {'name': 'regularMarketDayRange', 'type': 'STRING'},
            {'name': 'regularMarketChange', 'type': 'FLOAT'},
            {'name': 'regularMarketChangePercent', 'type': 'FLOAT'},
            {'name': 'regularMarketTime', 'type': 'STRING'},
            {'name': 'regularMarketVolume', 'type': 'FLOAT'},
            {'name': 'regularMarketPreviousClose', 'type': 'FLOAT'},
            {'name': 'regularMarketOpen', 'type': 'FLOAT'},
            {'name': 'averageDailyVolume10Day', 'type': 'FLOAT'},
            {'name': 'averageDailyVolume3Month', 'type': 'FLOAT'},
            {'name': 'fiftyTwoWeekLowChange', 'type': 'FLOAT'},
           # {'name': 'fiftyTwoWeekLowChangePercent', 'type': 'FLOAT'},
            {'name': 'fiftyTwoWeekRange', 'type': 'STRING'},
            {'name': 'fiftyTwoWeekHighChange', 'type': 'FLOAT'},
            {'name': 'fiftyTwoWeekHighChangePercent', 'type': 'FLOAT'},
            {'name': 'fiftyTwoWeekLow', 'type': 'FLOAT'},
            {'name': 'fiftyTwoWeekHigh', 'type': 'FLOAT'},
            {'name': 'twoHundredDayAverage', 'type': 'FLOAT'},
            {'name': 'twoHundredDayAverageChange', 'type': 'FLOAT'},
            {'name': 'twoHundredDayAverageChangePercent', 'type': 'FLOAT'}
        ]
        orientacion = 'replace'
        # print(df_cabecalho.dtypes)
        upload_df_bq(df_cabecalho, nome_schema, nome_tabela, data_structure, orientacion)
        print_msg('Dados de cabecalho importados com sucesso!')
    except Exception as e:
        print_msg(f'ERRO Rotina Ibovespa -> {e}')
    finally:
        sendEmail()

main()