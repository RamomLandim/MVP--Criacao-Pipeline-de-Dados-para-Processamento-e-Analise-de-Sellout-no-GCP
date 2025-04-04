# MVP--Criacao-Pipeline-de-Dados-para-Processamento-e-Analise-de-Sellout-no-GCP
Este projeto implementa um pipeline de dados escalável no Google Cloud Platform (GCP) para a coleta, processamento e análise de dados de sellout de distribuidores. Ele engloba desde a recepção dos arquivos até a modelagem e visualização dos dados, garantindo qualidade e governança.

## Objetivo

O objetivo do projeto é criar um pipeline robusto e automatizado para processar e analisar os dados de sellout, otimizando a coleta e garantindo escalabilidade. O sistema permite a ingestão, tratamento e análise dos dados no BigQuery, possibilitando insights para o negócio.

## Tecnologias Utilizadas

Para desenvolver o pipeline, foram utilizadas diversas ferramentas do Google Cloud Platform e bibliotecas Python:

Google Cloud Storage - Armazena os arquivos de sellout enviados pelos distribuidores.

Google Cloud Functions - Detecta novos arquivos no Storage e inicia a ingestão.

BigQuery - Data Warehouse para armazenamento e análise dos dados.

Flask - Utilizado na simulação do site de upload de arquivos.

Pandas - Processamento e manipulação de dados.

## Arquitetura do Pipeline

Upload dos Arquivos: Os distribuidores fazem upload dos arquivos Excel via portal web.

Armazenamento: Os arquivos são salvos no Cloud Storage.

Ingestão: O Cloud Functions detecta novos arquivos e aciona o Dataflow.

Processamento: O Dataflow transforma os dados e os insere nas tabelas temporárias no BigQuery.

Modelagem: As tabelas são transformadas para um modelo estrela.

Dashboard: Os dados são analisados e visualizados em dashboards interativos.

## Como Executar

Grande parte do projeto foi desenvolvido no Google Cloud Plataform. 

Caso deseja simular a execução dos códigos em relação ao Site e o Job, além de vizualiar o Power BI, recomenda-se clonar o Repositório.
~~~
git clone https://github.com/RamomLandim/MVP--Criacao-Pipeline-de-Dados-para-Processamento-e-Analise-de-Sellout-no-GCP.git
~~~
