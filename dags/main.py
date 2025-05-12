import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
from datetime import datetime

from airflow.decorators import dag, task

from app.etl import CarregarDadosBigQuery, LerArquivo, TratarDataframe

load_dotenv()

##### Variáveis gerais

# Arquivo do setor Financeiro: Classificação de crédito dos clientes
caminho_classif_credito = os.getenv("CAMINHO_EXCEL_CLASSIF_CREDITO_CLIENTES")
nome_tabela_classif_credito = "excel_classificacao_credito_"
col_base_classif_credito = ["ID_CLIENTE", "NOME_PESSOA"]
col_valores_classif_credito = ["Analista 1", "Analista 2", "Gerente", "Score Externo"]

# Arquivo de Suprimentos e Qualidade: Avaliação de fornecedores
caminho_avaliacao_forn = os.getenv("CAMINHO_EXCEL_AVALIACAO_FORNECEDORES")

nome_tabela_avaliacao_forn = "excel_avaliacao_fornecedores_"

# O objetivo deste dicionário é criar o nome da tabela que será salva no big query
caminho_raiz_nome_arquivos = {
    caminho_classif_credito: nome_tabela_classif_credito,
    caminho_avaliacao_forn: nome_tabela_avaliacao_forn,
}

##### DAGs


@dag(
    dag_id="processar_arquivo_classifi_credito",
    description="Responsável pelo processamento do arquivo financeiro Classificação de Crédito.xlsx",
    schedule="0 10 * * *",  # Executa diariamente às 07h (UTC -3)
    start_date=datetime(2025, 5, 7),
    catchup=False,
)
def ExecutarPipelineClassifCredito():

    @task
    def extract():
        conteudo_planilha = LerArquivo(
            caminho=caminho_classif_credito, aba="Dados", coluna_dados="ID do Cliente"
        )

        return conteudo_planilha

    @task
    def transform(conteudo_planilha):
        nome_arquivo, df_tabela_dados = TratarDataframe(
            conteudo=conteudo_planilha,
            colunas_base=col_base_classif_credito,
            colunas_valores=col_valores_classif_credito,
            nome_tabela=caminho_raiz_nome_arquivos,
        )

        return df_tabela_dados, nome_arquivo

    @task
    def load(resultado_transformacao):
        dataframe, nome_arquivo = resultado_transformacao
        CarregarDadosBigQuery(dataframe=dataframe, nome_tabela=nome_arquivo)

    dados = extract()
    resultado = transform(dados)
    load(resultado)


ExecutarPipelineClassifCredito()
