import os

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

caminho_classif_credito = os.getenv("CAMINHO_EXCEL_CLASSIF_CREDITO_CLIENTES")
caminho_avaliacao_forn = os.getenv("CAMINHO_EXCEL_AVALIACAO_FORNECEDORES")


def LerArquivo(caminho: str, aba: str, coluna_dados: str) -> pd.DataFrame:
    """
    Função destinada a leitura primária do arquivo Excel,
    cujo objetivo é retornar um dicionario contendo
    alguns metadados e um dataframe com os dados tabulares
    """

    df = pd.read_excel(caminho, sheet_name=aba, header=None)

    conteudo_planilha = {}

    metadados = {
        "Data:": "DATA_AVALIACAO",
        "Revisor:": "REVISOR",
        coluna_dados: "DADOS",
    }

    for nome_col, col_chave in metadados.items():

        for col in df.columns:

            check_metadados = df[col].astype(str).str.contains(nome_col, na=False)

            if check_metadados.any():

                coluna = col + 1
                linha = df[check_metadados].index[0]

                if nome_col == coluna_dados:

                    df_selecionado = df.iloc[linha:, col:]
                    df_selecionado.columns = df_selecionado.iloc[0]
                    df_selecionado = df_selecionado[1:].reset_index(drop=True)

                    conteudo_planilha[col_chave] = df_selecionado

                else:

                    resultado = df.iloc[linha, coluna]

                    if col_chave == "DATA_AVALIACAO":

                        resultado = pd.to_datetime(resultado, format="%d/%m/%Y").date()

                    conteudo_planilha[col_chave] = resultado

                break

    return conteudo_planilha


conteudo = LerArquivo(caminho_classif_credito, "Dados", "ID do Cliente")
