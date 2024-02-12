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