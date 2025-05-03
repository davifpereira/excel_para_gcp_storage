from datetime import date

import pandas as pd

from app.main import TratarDataframe


# Teste da segunda função do script main
def test_TratarDataframe():

    df = pd.DataFrame(
        [
            [None, "Critérios:", 0.2, 0.1, 0.3, 0.4],
            [
                "ID do Cliente",
                "Nome da Pessoa",
                "Analista 1",
                "Analista 2",
                "Gerente",
                "Score Externo",
            ],
            [111, "Pessoa1", 683, 932, 409, 900],
            [112, "Pessoa2", 512, None, 651, 707],
        ]
    )

    conteudo = {
        "DATA_AVALIACAO": date(2025, 4, 19),
        "REVISOR": "Gerente",
        "DADOS": df,
        "CAMINHO": "exemplo/",
    }

    caminho_raiz_nome_arquivos = {"exemplo/": "exemplo_"}

    colunas_base = ["ID_CLIENTE", "NOME_PESSOA"]
    colunas_valores = ["Analista 1", "Analista 2", "Gerente", "Score Externo"]

    # Nome da tabela e dataframe resultante da aplicação da função
    nome_arquivo_resultante, df_resultante = TratarDataframe(
        conteudo=conteudo,
        colunas_base=colunas_base,
        colunas_valores=colunas_valores,
        nome_tabela=caminho_raiz_nome_arquivos,
    )

    tabela_esperada = {
        "ID_CLIENTE": [111, 112, 111, 112, 111, 112, 111, 112],
        "NOME_PESSOA": [
            "Pessoa1",
            "Pessoa2",
            "Pessoa1",
            "Pessoa2",
            "Pessoa1",
            "Pessoa2",
            "Pessoa1",
            "Pessoa2",
        ],
        "AGENTE": [
            "Analista 1",
            "Analista 1",
            "Analista 2",
            "Analista 2",
            "Gerente",
            "Gerente",
            "Score Externo",
            "Score Externo",
        ],
        "SCORE": [683, 512, 932, None, 409, 651, 900, 707],
        "PESO_PERCENTUAL": [0.2, 0.2, 0.1, 0.1, 0.3, 0.3, 0.4, 0.4],
        "DATA_AVALIACAO": ["19/04/2025"] * 8,
        "REVISOR": ["Gerente"] * 8,
    }

    # Dataframe esperado como resultado da função
    df_esperado = pd.DataFrame(tabela_esperada)
    df_esperado["DATA_AVALIACAO"] = pd.to_datetime(
        df_esperado["DATA_AVALIACAO"]
    ).dt.date

    # Certifica se o nome da tabela gerado pela função condiz com o que se espera
    assert nome_arquivo_resultante == "exemplo_2025_04_19"
    # Verifica se os dataframes resultante e esperado são idênticos
    pd.testing.assert_frame_equal(df_resultante, df_esperado, check_dtype=False)
