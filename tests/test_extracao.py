from datetime import date

import numpy as np
import pandas as pd
import pytest

from app.main import LerArquivo


@pytest.fixture
def planilha_exemplo(tmp_path):

    caminho_arquivo = tmp_path / "exemplo.xlsx"
    dados = [
        [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [np.nan, np.nan, "Data:", "19/04/2025", np.nan, np.nan, np.nan],
        [np.nan, np.nan, "Revisor:", "Pessoa", np.nan, np.nan, np.nan],
        [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        [
            np.nan,
            "ID do Cliente",
            "Nome da Pessoa",
            "Analista 1",
            "Analista 2",
            "Gerente",
            "Score Externo",
        ],
        [np.nan, 111, "Adriana Morales", 683, 932, 409, 900],
        [np.nan, 112, "Agustina Méndez", 512, np.nan, 651, 707],
        [np.nan, 113, "Alejandro Ramírez", 802, 781, 603, 648],
    ]
    df = pd.DataFrame(dados)
    df.to_excel(caminho_arquivo, index=False, header=False)

    return caminho_arquivo


def test_LerArquivo(planilha_exemplo):

    resultado = LerArquivo(
        caminho=str(planilha_exemplo), aba="Sheet1", coluna_dados="ID do Cliente"
    )

    assert "DATA_AVALIACAO" in resultado
    assert resultado["DATA_AVALIACAO"] == date(2025, 4, 19)

    assert "REVISOR" in resultado
    assert resultado["REVISOR"] == "Pessoa"

    assert "DADOS" in resultado
    conteudo_dados = resultado["DADOS"]
    assert conteudo_dados.shape[0] == 3
