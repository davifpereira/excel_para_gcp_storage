import os

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from pandera.errors import SchemaError

from app.schema import SchemaAvaliacaoForn, SchemaClassifCredito

load_dotenv()

id_conjunto_dados_bigquery = os.getenv("ID_CONJUNTO_DADOS_BIGQUERY")

##### Funções


def LerArquivo(caminho: str, aba: str, coluna_dados: str) -> pd.DataFrame:
    """
    Função destinada a leitura primária do arquivo Excel,
    cujo objetivo é retornar um dicionario contendo alguns metadados e
    um dataframe com os dados tabulares
    """

    df = pd.read_excel(caminho, sheet_name=aba, header=None, dtype=str)

    conteudo_planilha = {}

    metadados = {
        "Data:": "DATA_AVALIACAO",
        "Revisor:": "REVISOR",
        coluna_dados: "DADOS",
    }

    # Pesquisa coluna a coluna para encontrar em qual está as palavras chaves do dicionario acima
    for nome_col, col_chave in metadados.items():

        for col in df.columns:

            # Verifica se a palavra chave está presente na coluna iterada
            check_metadados = df[col].str.contains(nome_col, na=False)

            # caso sim, guarda o número da coluna e da linha
            if check_metadados.any():

                coluna = col + 1
                linha = df[check_metadados].index[0]

                # Guarda num dicionário a data de avaliação, o revisor e os dados de avaliação
                if nome_col == coluna_dados:

                    # Dados de avaliação
                    df_selecionado = df.iloc[linha - 1 :, col:].reset_index(drop=True)

                    conteudo_planilha[col_chave] = df_selecionado

                else:

                    resultado = df.iloc[linha, coluna]

                    # Data de avaliação e Revisor
                    if col_chave == "DATA_AVALIACAO":

                        resultado = pd.to_datetime(resultado).date()

                    conteudo_planilha[col_chave] = resultado

                break

        # Retém ainda o caminho do arquivo
        conteudo_planilha["CAMINHO"] = caminho

    return conteudo_planilha


def TratarDataframe(
    conteudo: dict, colunas_base: list, colunas_valores: list, nome_tabela: dict
) -> pd.DataFrame:
    """
    É resposável pelo tratamento dos dados com base nestas ações:

    - Elabora um dataframe para concetrar o peso percentual da avaliação de cada agente;
    - Delimita o dataframe principal (aquele que contém os dados das avaliações, propriamente);
    - Transforma as colunas de agentes e suas avaliações em linhas (unpivot);
    - Realiza o merge do dataframe principal com o de pesos percentuais;
    - Inclui duas colunas no dataframe: data de avaliação e revisor.
    """

    # Dicionário que será destinado à alteração do nome das colunas
    altera_nome_colunas = {
        "ID do Cliente": "ID_CLIENTE",
        "Nome da Pessoa": "NOME_PESSOA",
    }

    # Seleciona os dados de avaliação,
    # para iniciar a separação dos critérios percentuais e
    # das avaliações, propriamente
    df = conteudo["DADOS"]

    # Dados dos critérios percentuais
    df_criterios = df.iloc[0:2]

    # Realiza essa iteração para excluir os dados nulos
    for col in df_criterios.columns:

        check_criterios = (
            df_criterios[col].astype(str).str.contains("Critérios", na=False)
        )

        if check_criterios.any():

            coluna = col + 1

            df_criterios = df.iloc[0:2, coluna:]
            df_criterios.columns = df_criterios.iloc[1]
            df_criterios = df_criterios.iloc[0:1].reset_index(drop=True)

            break

    df_criterios = pd.melt(
        frame=df_criterios,
        value_vars=df_criterios.columns,
        var_name="AGENTE",
        value_name="PESO_PERCENTUAL",
    )

    # Tabela dos dados de avaliação
    df.columns = df.iloc[1]
    df_tabela_dados = df[2:].reset_index(drop=True)

    df_tabela_dados = df_tabela_dados.rename(columns=altera_nome_colunas)

    df_tabela_dados = pd.melt(
        frame=df_tabela_dados,
        id_vars=colunas_base,
        value_vars=colunas_valores,
        var_name="AGENTE",
        value_name="SCORE",
    )

    # Junta a tabela dos dados de avaliação com a que contém
    # o peso percentual da avaliação de cada agente
    df_tabela_dados = df_tabela_dados.merge(right=df_criterios, on="AGENTE")

    # Incluir uma coluna para a data de avaliação
    data_avaliacao = conteudo["DATA_AVALIACAO"]
    df_tabela_dados["DATA_AVALIACAO"] = data_avaliacao

    # Inclui uma coluna para o revisor da avaliação
    revisor = conteudo["REVISOR"]
    df_tabela_dados["REVISOR"] = revisor

    # Define o nome que a tabela terá no data warehouse
    caminho_arquivo = conteudo["CAMINHO"]
    raiz_nome_arquivo = nome_tabela[caminho_arquivo]
    nome_arquivo = raiz_nome_arquivo + data_avaliacao.strftime("%Y_%m_%d")

    # Valida os contratos de dados
    try:
        if "classificacao_credito" in raiz_nome_arquivo:

            df_tabela_dados = SchemaClassifCredito.validate(df_tabela_dados)

        elif "avaliacao_fornecedores" in raiz_nome_arquivo:

            df_tabela_dados = SchemaAvaliacaoForn.validate(df_tabela_dados)

    except SchemaError as e:
        print(f"Falha na validação do esquema: {e}")

    return nome_arquivo, df_tabela_dados


def CarregarDadosBigQuery(dataframe: pd.DataFrame, nome_tabela: str):
    """
    Cria uma tabela no Google Big Query para gravar dados oriundos de um dataframe
    """

    client = bigquery.Client()

    id_tabela_bigquery = f"{id_conjunto_dados_bigquery}.{nome_tabela}"

    configura_chamada_api = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    executa_chamada_api = client.load_table_from_dataframe(
        dataframe=dataframe,
        destination=id_tabela_bigquery,
        job_config=configura_chamada_api,
    )

    executa_chamada_api.result()
