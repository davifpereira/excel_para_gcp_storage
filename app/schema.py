import pandera as pa
from pandera.typing import Series

# Contrato de dados


# Schema para a tabela de classificação de crédito dos clientes
class SchemaClassifCredito(pa.DataFrameModel):
    ID_CLIENTE: Series[int]
    NOME_PESSOA: Series[str]
    AGENTE: Series[str]
    SCORE: Series[float] = pa.Field(ge=0, nullable=True)
    DATA_AVALIACAO: Series[pa.DateTime]
    REVISOR: Series[str]
    PESO_PERCENTUAL: Series[float] = pa.Field(ge=0.05, le=0.95)

    class Config:
        strict = True
        coerce = True


# Schema para a tabela de avaliação de fornecedores
class SchemaAvaliacaoForn(pa.DataFrameModel):
    ID_FORNECEDOR: Series[int]
    NOME_FORNECEDOR: Series[str]
    DATA_INSPECAO: Series[pa.DateTime]
    AGENTE: Series[str]
    SCORE: Series[float] = pa.Field(ge=0, nullable=True)
    DATA_AVALIACAO: Series[pa.DateTime]
    REVISOR: Series[str]
    PESO_PERCENTUAL: Series[float] = pa.Field(ge=0.05, le=0.95)

    class Config:
        strict = True
        coerce = True
