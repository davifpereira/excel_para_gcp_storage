# Projeto de Integração de Dados com Python, Airflow e BigQuery

## Objetivos do projeto

Este projeto foi desenvolvido para resolver um problema comum em ambientes corporativos: a dificuldade de consolidar dados operacionais dispersos em planilhas Excel, que complementam (ou até substituem) funcionalidades de um ERP.

Por meio de um pipeline totalmente automatizado, este projeto realiza:

- **Mapeamento e validação de dados** provenientes de dois arquivos Excel;
- **Pequenas transformações nesses dados**, aplicando regras de negócio específicas;
- **Carga dos dados em um Data Warehouse (Google BigQuery)**, tornando-os prontos para serem integrados a pipeline de dados robustos, que visem integrar diversas fontes de dados.

## Serviços e ferramentas utilizadas

- **Python** - Linguagem principal do projeto, na qual se utilizaram bibliotecas como:
  - `openpyxl` – Manipulação de arquivos Excel;
  - `pandas` e `pandera` – Processamento, validação e transformação de dados;
  - `google-cloud-bigquery` – Integração e carga no BigQuery;
  - `apache-airflow` – Orquestração do pipeline;
  - `mkdocs` – Documentação técnica automatizada.

- **Apache Airflow** - Utilizado para a orquestração das cargas de dados, mediante o uso de DAGs e task para tal finalidade. Sua instalação se deu por meio do Astro CLI;

- **Docker** - É quem containeriza o ambiente do Airflow, garantindo portabilidade e reprodutibilidade;

- **Google BigQuery** - Data Warehouse auto gerenciável na nuvem, onde os dados finais são armazenados.

