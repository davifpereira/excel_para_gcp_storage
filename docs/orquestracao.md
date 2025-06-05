# Orquestração

A orquestração do pipeline é realizada utilizando o **Apache Airflow**, com duas DAGs principais:

- Processamento da **Classificação de Crédito de Clientes**;
- Processamento da **Avaliação de Fornecedores**.

---

## Estrutura das DAGs

### Variáveis

- **Caminhos dos arquivos:** definidos via variáveis de ambiente no arquivo `.env`.
- **Nomes das tabelas:** construídos dinamicamente com base no nome dos arquivos e nas datas de avaliação.
- **Colunas bases e colunas de valores:** definem quais colunas permanecem fixas e quais são utilizadas no unpivot dos dados.

---

## DAG: `processar_arquivo_classifi_credito`

### Descrição
Realiza a extração, transformação e carga dos dados do arquivo **Classificação de Crédito.xlsx**, pertencente ao setor financeiro.

### Schedule
Executa diariamente às 07h (UTC-3) → `0 10 * * *`

---

## DAG: `processar_arquivo_avaliacao_forn`

### Descrição
Executa o processamento dos dados do arquivo **Avaliação de Fornecedores.xlsx**, utilizado pelos setores de Suprimentos.

### Schedule
Executa diariamente às 07h (UTC-3) → `0 10 * * *`