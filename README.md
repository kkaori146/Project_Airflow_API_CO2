# Projeto Imigrantes

- Projeto que visa testar os conhecimentos em airflow extraindo dados de uma API
- Análise preliminar sobre o ciclo sazonal global diário e valor de tendência entre os anos de 2012 a 2022
- Exportação do resultado (arquivo parquet) da subpasta pesquisa para o PostgreSQL

## Fonte

https://global-warming.org/


## Ferramentas

<div align="center">
<p float="left">
  <img src="https://user-images.githubusercontent.com/83531935/202240019-106b54cb-b397-4bcc-a29a-e8ab55dcca85.png" width="180" />
  <img src="https://user-images.githubusercontent.com/83531935/202240028-cd1716fe-dfd5-4484-a9c1-9422da702468.png" width="380" /> 
  <img src="https://user-images.githubusercontent.com/83531935/202240030-59908174-d35d-4a4f-aeb8-9622420886f9.png" width="200" />
  <img src="https://user-images.githubusercontent.com/83531935/202240035-8d3d3582-b222-472d-baa8-1ed9551f2b0e.png" width="180" />
</p>
</div>


## Comandos

- Implementação das modificações no docker-compose.yaml (PostgreSQL)

  **__$\textcolor{darkgreen}{\text{docker-compose up -d --no-deps --build postgres}}$__**

- Solucionando problema no import do PostgresOperator dentro do vscode

  **__$\textcolor{darkgreen}{\text{pip install 'apache-airflow[postgres]}}$__**

- Inicialização rápida do airflow

  **__$\textcolor{darkgreen}{\text{docker-compose up airflow-init}}$__**

- Implementação das modificações na DAG ou docker-compose.yaml

  **__$\textcolor{darkgreen}{\text{docker-compose up}}$__**

## Etapas Gerais

1) Importação das bibliotecas e ferramentas necessárias
2) Verificação da disponibilidade dos dados da API
3) Extração dos dados na API
4) Armazenamento dos dados na pasta de dados_brutos em formato json 
5) Pré-tratamento dos dados utilizando Google Colab (normalização, tratamento e armazenamento dos dados em outros formatos e uso do groupBy para outras análises)
6) Desenvolvimento da DAG
7) Envio do dataset da subpasta analise para o PostgreSQL

## Conexões

- Conexão criada para conectar à API

<div align="center">
<img src="" width=1000px > </div>

- Conexão criada para conectar ao PostgreSQL

<div align="center">
<img src="" width=1000px > </div>

## Resultados

- Dependências

<div align="center">
<img src="" width=1000px > </div>


<br>

- PostgreSQL

<div align="center">
<img src="" width=1000px > </div>

<br>
<br>
<hr/>

<div align="right"><p>Novembro, 2022</p></div>
