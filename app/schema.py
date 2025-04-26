from datetime import date

import pandera as pa
from pandera import Field
from pandera.typing import Series


class SchemaClassifCredito(pa.SchemaModel):
    data: Series[date] = Field(alias="DATA_AVALIACAO")
    revisor: Series[str] = Field(alias="REVISOR")
    id_do_cliente: Series[int] = Field(alias="ID do Cliente")
    nome_da_pessoa: Series[str] = Field(alias="Nome da Pessoa")
    analista_1: Series[int] = Field(alias="Analista 1", ge=0)
    analista_2: Series[int] = Field(alias="Analista 2", ge=0, nullable=True)
    gerente: Series[int] = Field(alias="Gerente", ge=0)
    score_externo: Series[int] = Field(alias="Score Externo", ge=0)

    class Config:
        coerce = True
        strict = True
