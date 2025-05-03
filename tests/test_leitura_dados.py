from datetime import date
from pathlib import Path

from app.main import LerArquivo


# Teste da primeira função do script main
def test_LerArquivo():

    # Usa o arquivo excel de exemplo presente nesta pasta
    caminho_pasta = Path(__file__).parent
    caminho_arquivo = caminho_pasta / "exemplo.xlsx"

    resultado = LerArquivo(
        caminho=caminho_arquivo, aba="Sheet1", coluna_dados="ID do Cliente"
    )

    assert "DATA_AVALIACAO" in resultado
    # Verifica se a data de avaliação foi captada corretamente
    assert resultado["DATA_AVALIACAO"] == date(2025, 4, 19)

    assert "REVISOR" in resultado
    # Verifica se a pessoa revisor foi captada corretamente
    assert resultado["REVISOR"] == "Gerente"

    assert "DADOS" in resultado
    conteudo_dados = resultado["DADOS"]
    # Verifica se o dataframe possui 5 linhas
    assert conteudo_dados.shape[0] == 5
    # Verifica se o dataframe possui 6 colunas
    assert conteudo_dados.shape[1] == 6
