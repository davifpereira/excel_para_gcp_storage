import os
from datetime import datetime

from airflow.decorators import dag, task
from dotenv import load_dotenv

from app.etl import CarregarDadosBigQuery, LerArquivo, TratarDataframe

id_conjunto_dados_bigquery = os.getenv("ID_CONJUNTO_DADOS_BIGQUERY")

##### Variáveis gerais

load_dotenv()

# Arquivo do setor Financeiro: Classificação de crédito dos clientes
caminho_classif_credito = os.getenv("CAMINHO_EXCEL_CLASSIF_CREDITO_CLIENTES")
nome_tabela_classif_credito = "excel_classificacao_credito_"
col_base_classif_credito = ["ID_CLIENTE", "NOME_PESSOA"]
col_valores_classif_credito = ["Analista 1", "Analista 2", "Gerente", "Score Externo"]

# Arquivo de Suprimentos e Qualidade: Avaliação de fornecedores
caminho_avaliacao_forn = os.getenv("CAMINHO_EXCEL_AVALIACAO_FORNECEDORES")

nome_tabela_avaliacao_forn = "excel_avaliacao_fornecedores_"


id_conjunto_dados_bigquery = os.getenv("ID_CONJUNTO_DADOS_BIGQUERY")

# O objetivo deste dicionário é criar o nome da tabela que será salva no big query
caminho_raiz_nome_arquivos = {
    caminho_classif_credito: nome_tabela_classif_credito,
    caminho_avaliacao_forn: nome_tabela_avaliacao_forn,
}
