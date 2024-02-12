# Para execução do ambiente de teste:
1 - Ter instalado o Docker Desktop

2 - Clonar este repositório para a máquina local

3 - Executar "docker-compose up -d" na pasta aonde foi clonado os documentos:
![image](https://github.com/matheu-spereira/validador/assets/33911601/8f970f40-d273-4e63-b334-0ca88d2bf715)
4 - No navegador entrar no link, senha: "token" : [http://localhost:8888/lab/tree/notebooks/Valida%C3%A7%C3%A3o%20estrutura/valida_tabelas.ipynb](http://localhost:8888/lab/tree/notebooks)

5 - Clique em terminal e execute o comando: pip3 install --upgrade elyra-pipeline-editor-extension
![image](https://github.com/matheu-spereira/validador/assets/33911601/bff78289-7ef0-4ec8-8f6e-ef3c9cbd8c49)

6 - Após finalizar a instalação, pare e reinicie o container

![image](https://github.com/matheu-spereira/validador/assets/33911601/776f3f27-590c-4925-97ff-19f9044cd40d)

7 - Retorne ao link da etapa 4. Será instaldo o componente Generic pipeline editor.
![image](https://github.com/matheu-spereira/validador/assets/33911601/2e63f64c-1c4f-4af0-a62f-f1f407a28a78)

8 - No ambiente de teste está sendo utilizando a base de dados de avaliacoes, para criar uma tabela no banco vá em: /notebooks/dataset_avaliacoes/

9 - Clique em 0_pipeline_source_dw_avaliacoes.pipeline

10 - Clique em executar depois ok e ok. Aguarde a finalização do pipeline.
![image](https://github.com/matheu-spereira/validador/assets/33911601/357f8da5-d464-4c33-a3db-e6131bb74d42)

11 -  Com isso o dataset de avaliações usado no laboratório será criado no banco de dados postgree
![image](https://github.com/matheu-spereira/validador/assets/33911601/27f5fdf7-0da5-40d5-b380-e1bb1e5f9583)

12 - Agora o ambiente está configurado para testes da aplicação de validação do banco.
Localização: /notebooks/Validação estrutura/
Notebook para chamar a classe: valida_tabelas.ipynb
Classe python: validador.py
Dicionário de dados: a.xlsx

![image](https://github.com/matheu-spereira/validador/assets/33911601/513c5c60-e654-420b-8b55-8af1ef2e2740)



# Entendimento 
 Este notebook tem como objetivo validar se as colunas das Views em um banco SQL SERVER estão de acordo com o Dicionário de Dados baseado nas colunas dos Endpoints da API do Caravel
 A função desenvolvida recebe as seguintes variáveis:
 file_name = Localização do dicionário de dados (.xlsx);
 sheet_name = Nome da aba da planilha onde está contida o dicionário de dados da tabela a ser validada;
 query_sqlserver = Consulta SQL que irá retornar os metadados da tabela que será validada;
 engine = String de conexão do banco;

# Funcionamento:
 A função "process_dataframes", realiza um left join entre o df que contém os metadados da tabela do banco com o df do dicionário de dados através da coluna que representa o nome das colunas de ambos os dataframes("column_name"). Após o join é retornado as colunas correspondentes do dicionário de dados que são (nome de coluna, tipo de dado e se é null ou não). A partir da obtenção dessas informaçãoes é realizado a validação dos metadados. Também é apresentado as colunas que estão no dicionário de dados mas não foram encontradas na tabela do banco, seja por não existirem ou por terem nomes diferentes entre si.

# Pré-Requisitos:
 O dicionários de dados devem ter uma tabela com as seguintes colunas: "column_name", "data_type" e "is_nullable" para realização dos join e validações.

# Regras adicionais aplicadas:
 Durante a validação da tabela do banco, os tipos de dados (char, varchar, text, nchar, nvarchar e ntext) são consolidados como varchar.
 Durante a validação da tabela do banco, os tipos de dados (bigint, numeric, smallint, int e numeric) são consolidados como int.
 Obs: É possível ver essa consolidação na coluna "dtype_consolidado"

# Novos itens a desenvolver:
Retornar primeiras linhas da tabela
Retornar USUÁRIO que executou
retornar CATEGORIA DE BANCOS? SQL SERVER, ORACLE, POSTGREE?
VARIAVEIS DE CONEXÃO, NOME, HOST, VALIDADOR, NOME ARQUVOS. IMPRIMIT AO FINAL
COLORIR OK OU NÃOOK

V2
Acrescentar função que permite ler da API, CSV, OUTRAS ORIGENS ALÉM DO SQL SERVER
