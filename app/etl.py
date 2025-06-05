import os
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from pandera.errors import SchemaError
from functools import wraps
from app.schema import SchemaAvaliacaoForn, SchemaClassifCredito
import logging

load_dotenv()

id_conjunto_dados_bigquery = os.getenv("ID_CONJUNTO_DADOS_BIGQUERY")

logger = logging.getLogger("airflow.task")

##### Funções

def capturar_erros(funcao):
    '''
    Responsável pelo mapeamento dos eventuais erros que podem ocorrer durante o processo.

    Parameters:
        funcao (function): caminho do arquivo Excel contendo os dados da avaliação.

    Returns:
        retorna a função original encapsulada com o tratamento de erros.
    '''

    @wraps(funcao)
    def wrapper(*args, **kwargs):
        try:

            return funcao(*args, **kwargs)
        
        except Exception as e:

            logger.error(f"Ocorreu um erro na função '{funcao.__name__}':\n\n{e}")

            raise SystemExit()

    return wrapper


def ler_arquivo(caminho: str, aba: str, coluna_dados: str) -> dict:
    """
    Lê um arquivo Excel e extrai tanto os dados tabulares quanto os metadados da avaliação.

    Parameters:
        caminho (str): caminho do arquivo Excel contendo os dados da avaliação.
        aba (str): nome da aba que contém os dados;
        coluna_dados (str): valor presente na célula de cabeçalho que indica onde começa a tabela de dados.

    Returns:
        conteudo_planilha (dict): contém tanto os metadados das avaliações quanto o dataframe de dados, propriamente. 
        Seus elementos são:
            - Data da avaliação;
            - Nome do revisor;
            - Dados tabulares;
            - Caminho do arquivo.      
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


@capturar_erros
def tratar_dataframe(
    conteudo: dict, colunas_base: list, colunas_valores: list, nome_tabela: dict
) -> tuple[str, pd.DataFrame]:
    """
    É resposável pelo tratamento dos dados e, portanto, executa estas ações:

    - Cria um dataframe com os pesos percentuais de cada agente;
    - Realiza o unpivot dos dados de avaliação;
    - Faz o merge dos dados de avaliação com os pesos;
    - Insere colunas de data da avaliação e revisor;
    - Define o nome da tabela de destino;
    - Realiza a validação dos dados de acordo com o schema.

    Parameters:
        conteudo (dict): dicionário obtido pelo retorno da função ler_arquivo.
        colunas_base (list): lista de colunas que permanecerão fixas durante o processo de unpivot.
        colunas_valores (list): lista de colunas que serão transformadas em linhas no processo de unpivot.
        nome_tabela (dict): dicionário que contempla a relação entre o caminho do arquivo e o nome base da tabela no BigQuery.

    Returns:
        tuple[str, pd.DataFrame]:
            nome_arquivo (str): nome final da tabela no BigQuery (incluindo a data no sufixo).
            df_tabela_dados (pd.DataFrame): dataframe tratado e validado, pronto para ser carregado no BigQuery.
    """

    # Dicionário que será destinado à alteração do nome das colunas
    altera_nome_colunas = {
        "ID do Cliente": "ID_CLIENTE",
        "Nome da Pessoa": "NOME_PESSOA",
        "ID do Fornecedor": "ID_FORNECEDOR",
        "Fornecedor": "NOME_FORNECEDOR",
        "Data da Inspeção": "DATA_INSPECAO"
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
        logger.error(f"Falha na validação do esquema: {e}")

        raise SystemExit()

    return nome_arquivo, df_tabela_dados


@capturar_erros
def carregar_dados_bigquery(dataframe: pd.DataFrame, nome_tabela: str):
    """
    A partir de um dataframe, carrega dados numa tabela do Google Big Query, sobrescrevendo-a caso já exista.

    Parameters:
        dataframe (pd.DataFrame): dataframe contendo dados tratados e validados, prontos para serem carregados no BigQuery.
        nome_tabela (str): nome da tabela de destino no BigQuery.

    Returns:
        None
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

    qtd_linhas = dataframe.shape[0]
    logger.info(f"{qtd_linhas} linhas carregadas na tabela {nome_tabela} do dataset {id_conjunto_dados_bigquery}.")