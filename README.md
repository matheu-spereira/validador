# Entendimento Prova prática - Analista de Big Data

Bibliotecas adicionais instaladas no ambiente para execução dos notebooks
- pip3 install --upgrade elyra-pipeline-editor-extension (utilizada para orquestração dos notebooks jupyter)

Necessita reiniciar o docker, caso instalado por meio dele

## Documentação e Etapas
### Diretórios
A estrutura de diretórios dos datasets foi montada pensando no tratamento de forma independente de cada etapa de workload, passando pelas etapas de gravação de dados na camada Bronze, Silver, Gold e gravação no DW. Cada notebook de cada dataset está contido em seu próprio diretório.

Na estrutura montada, existe um diretório temporário de armazenamento chamado "temporary_blobs". Seu objetivo é permitir a gravação dos dataset em sua pasta para apoiar em tarefas de escrita no miniIO. Dessa forma o diretório é utilizado como ponte entre algum dataset tratado e sua gravação no bucket ou subpasta no minIO.

Dentro da raiz do diretório de cada dataset existe um componente chamado "0_pipeline_source_dw_dataset.pipeline", reponsável por orquestrar a execução de todos os notebooks entre as camadas de ETL.

Modelo:

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/90a48a1c-f398-498d-9eaa-6de0bf6b6d6b)



## Etapa 1: Extração
Os notebooks de extração são responáveis por 
* Ler os endpoints
* Inserirem uma coluna de data chamada "etl_date"
* Gravarem no diretório temporário (temporary_blobs)
* Gravarem o dataset no formato parquet em pastas versionadas no bucket a partir do seguinte diretório: bronze / dataset / dataset+data
* Ao Final os dados do diretório temporário (temporary_blobs) são excluídos.
  
Exemplo:

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/63fdb066-70d4-4120-98ea-1180747f476b)


## Etapa 2: Transformação

Os notebooks de Transformação são responáveis por 
* Ler os dados do bucket da camada bronze
* Realizeram tratamento de dados voltados para estrutura da tabela, como tipagem de colunas ou substituição de valores por exemplo.
* Gravarem no diretório temporário (temporary_blobs)
* Gravarem o dataset no formato parquet em pastas versionadas no bucket a partir do seguinte diretório: silver / dataset / dataset+data
* Ao Final os dados do diretório temporário (temporary_blobs) são excluídos.
  
Exemplo:

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/147f0fd0-11b6-40b9-9535-74a9b5e709c5)


## Etapa 3: Refinamento

Os notebooks de Refinamento são responáveis por 
* Ler os dados do bucket da camada silver
* Realizeram o tratamento de dados voltados para as tabelas no DW, nessa etapa é filtrado o dataset mais recente baseado na coluna etl_date, visto que todos os datasets das camadas anteriores são versionados por data e possuem registro de escrita.
* Gravarem no diretório temporário (temporary_blobs)
* Gravarem o dataset no formato parquet sobrescrevendo toda a pasta bucket a partir do seguinte diretório: gold / dataset 
* Ao Final os dados do diretório temporário (temporary_blobs) são excluídos.

Como seu objetivo é preparar o dataset para gravação no DW foi pensado que para cada dataset que seja escrito na camada gold, os dados contidos nela sejam sobrescritos pelo o arquivo mais atualizado da camada silver baseado em sua coluna "etl_date".
  
Exemplo:

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/5e4f372a-98c5-4d57-b8ca-7d18b01fbc94)


## Etapa 4: Carga

Os notebooks de Carga são responáveis por 
* Ler os dados do bucket da camada Gold
* Gravarem no diretório temporário (temporary_blobs)
* Gravarem o dataset no DW, no banco de dados "prova". Para todos os datasets foi pensando a escrita no modo overwrite no banco.
* Ao Final os dados do diretório temporário (temporary_blobs) são excluídos.

Por não serem tabelas grandes e por serem voltadas a informações de alunos imagina-se que não tenha um fluxo  diário alto de inserção de dados, acredita-se que seja inserções baseados em semestres ou bimestres por isso foi optado a gravação sobrescrita. Em um cenário onde haja grande atualiações de dados de forma diária ou semanal, é interessante pensar em uma escrita em forma de append.

Exemplo:

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/1376263b-0952-481e-ba11-63863b70a937)



## Etapa 5: Execução dos pipelines

Os pipelines de Execução são responáveis por 
* Executarem os notebooks em ordem de gravação nas camadas. Bronze, Silver, Gold e DW
  
1 - 1_source_bronze_dataset.ipynb

2 - 2_bronze_silver_dataset.ipynb

3 - 3_silver_gold_dataset.ipynb

4 - 4_gold_dw_dataset.ipynb
    

Exemplo:

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/1c4f5a84-cf63-48b7-bbc9-570e6d4d84ee)

## Etapa 6: Leitura dos dados pelo banco de dados
Desenvolvido relatório no Power BI através do consumo do banco de dados postgres, essa é a última etapa do fluxo de dados desenvolvido para esse projeto. 
Nessa etapa os usários poderão desenvolver seus próprios relatórios utilizando alguma ferramenta de visualização de dados como o Power BI.

![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/3ed06bea-a30d-4301-b3c0-7f28af4091a0)


## Fluxo de dados e estrutura final do ambiente
### Fluxo de dados
![Fluxo operacional financeiro - Post Instagram](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/3e65ab63-c601-4a01-8d60-f3e0320b128c)

### Ambiente minIO
![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/8352b7c7-9f28-49db-a66a-9f02c77499cd)
![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/e856b45f-0b56-4555-83c3-60061bbcaeb2)
![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/a9ce5953-1537-462a-94a1-6638728f42a4)

### Ambiente Postgre
![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/53eaad55-9284-4206-8042-8bcdbaa5490d)

### Ambiente Jupyter
![image](https://github.com/SENAI-SD/prova-analista-big-data-pleno-00006-2024-061.785.871-39/assets/33911601/f8c3fc68-5862-476e-a93e-cc62639a4d0d)

