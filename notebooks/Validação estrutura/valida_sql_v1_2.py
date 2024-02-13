from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import pandas as pd
from rich.console import Console
from datetime import datetime, timedelta, timezone



class DataFrameProcessor:
    def __init__(self, file_name, sheet_name, schema, nome_tabela, engine):
        #Sessão Spark
        self.spark = SparkSession.builder \
            .appName("DataFrame Processor") \
            .getOrCreate()
        self.file_name = file_name #Caminho do dicionário de dados
        self.sheet_name = sheet_name #Aba do dicionário de dados
        self.schema = schema #Schema da tabela do banco
        self.nome_tabela = nome_tabela #Nome da tabela do banco
        self.query_sqlserver = f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{nome_tabela}';" #Consulta metadados
        #self.query_top = f"SELECT TOP 5 * FROM {self.schema}.{self.nome_tabela};" 
        self.query_top = f"SELECT * FROM {self.schema}.{self.nome_tabela} LIMIT 5;" #Top 5 linhas da tabela para sample
        self.engine = engine #Engine de conexão
        utc_offset = -3  # UTC-3
        self.execution_time = datetime.now(timezone(timedelta(hours=utc_offset))) #Gera hora e data da execução

    # Realiza a leitura do dicionário de dados para o DataFrame   
    def read_excel_to_df(self):
        dic_vw_pd = pd.read_excel(self.file_name, sheet_name=self.sheet_name)
        self.dic_vw = self.spark.createDataFrame(dic_vw_pd)
        
    # Consulta SQL para selecionar os metadados da tabela e gerar consulta com top 5 valores
    def execute_sql_to_df(self):
        df_pd = pd.read_sql(self.query_sqlserver, self.engine)
        self.df = self.spark.createDataFrame(df_pd)
        df_top = pd.read_sql(self.query_top, self.engine)
        self.df_top = self.spark.createDataFrame(df_top)

    # Condições para criar a nova coluna dtype_consolidado, consolida tipos de dados semelhantes como algo único para validação posteriormente    
    def process_dataframes(self):
        condicao_text = (self.df['data_type'].contains('char') |
                         self.df['data_type'].contains('varchar') |
                         self.df['data_type'].contains('text') |
                         self.df['data_type'].contains('nchar') |
                         self.df['data_type'].contains('nvarchar') |
                         self.df['data_type'].contains('ntext'))

        condicao_int = (self.df['data_type'].contains('bigint') |
                        self.df['data_type'].contains('numeric') |
                        self.df['data_type'].contains('smallint') |
                        self.df['data_type'].contains('int') |
                        self.df['data_type'].contains('numeric'))

        self.df = self.df.withColumn('dtype_consolidado',
                                     when(condicao_text, 'varchar')
                                     .when(condicao_int, 'int')
                                     .otherwise(self.df['data_type']))

        # Armazena consulta com top 5
        result_top = self.df_top


        # Realiza o left join da tabela do banco com dicionário, join pela coluna "column_name"
        result_detalhado = self.df.join(
            self.dic_vw, self.df["column_name"] == self.dic_vw["column_name"], "left") \
            .select(self.df["*"], self.dic_vw["column_name"].alias('dic_column_name'),
            self.dic_vw["data_type"].alias('dic_data_type'), self.dic_vw["is_nullable"].alias('dic_is_nullable'))

        # Realiza o left outer da tabela do dicionário, join pela coluna "column_name". Visando buscar as colunas que existem no dicionário mas não existem na tabela do banco
        result_existe_somente_dic = self.dic_vw.join(
            self.df, self.dic_vw["column_name"] == self.df["column_name"], "left_outer") \
            .filter(self.df["column_name"].isNull()) \
            .select(self.dic_vw["column_name"], self.dic_vw["data_type"], self.dic_vw["is_nullable"])


        # Adiciona as colunas adicionais com lógica condicional, comparando as colunas, tipagem e nulls com as mesmas infos do dicionário.        
        result_detalhado = result_detalhado.withColumn(
            "coluna_existente_no_dic?",
            when(col("dic_column_name").isNull(), "não").otherwise("sim")
        ).withColumn(
            "tipagem_igual_dic?",
            when(col("dtype_consolidado") == col("dic_data_type"), "sim").otherwise("não")
        ).withColumn(
            "null_igual_dic?",
            when(col("is_nullable") == col("dic_is_nullable"), "sim").otherwise("não")
        )

        
        # Validação consolidada, verificando os tres itens de metadados
        result_consolidado = result_detalhado.withColumn(
            "Metadados coerentes com dicionário?",
        when(
            (col("coluna_existente_no_dic?") == "sim") &  
            (col("tipagem_igual_dic?") == "sim") &  
            (col("null_igual_dic?") == "sim"),
            "Sim"
            ).otherwise("Não")
        ).withColumn(
            "Observação",
        when(
            (col("coluna_existente_no_dic?") == "não"),
            "Não encontrada no dicionário"
            ).when(
                (col("tipagem_igual_dic?") == "não") | 
                (col("null_igual_dic?") == "não"),
                "Tipagem ou nullable diferentes do dicionário"
            ).otherwise("OK")
        ).select('column_name', 'Metadados coerentes com dicionário?', 'Observação')

        # Inicialize a instância do console Rich
        console = Console()

        # Gera visualização Final
        console.rule("RESULTADO DA VALIDAÇÃO", style="bold blue")
        print('Nome da aba do dicionário de dados:', self.sheet_name) # Exibição do nome da aba do dicionário de dados
        print('Schema da tabela do banco:', self.schema) # Exibição do Schema da tabela do banco
        print('Nome da Tabela:', self.nome_tabela) # Exibição do Nome da tabela do banco
        print('Hora e data:', self.execution_time) # Exibição da Hora e data da execução

        # Função para inserir cor no resultado consolidado. Se valor for OK, retorne green. Caso contrário Red
        def neg_vermelho(val):
            color = 'LimeGreen' if val == 'OK' or val == 'sim' else 'red'
            return 'color: {0!s}'.format(color)

        # Exibindo os DataFrames Pandas de forma responsiva

        #Resultado consolidado a partir da tabela do banco
        console.rule("[bold green]RESUMO DA TABELA DO BANCO[/bold green]")
        display(result_consolidado.toPandas().style.applymap(neg_vermelho, subset=['Observação']))
        #Resultado das colunas presentes no dicionário não encontradas na tabela do banco
        console.rule("[bold green]COLUNAS DO DICIONÁRIO NÃO ENCONTRADAS NA TABELA DO BANCO[/bold green]")
        display(result_existe_somente_dic.toPandas())
        #Resultado da sample da tabela do banco, top 5 linhas
        console.rule("[bold green]TOP 5 LINHAS [/bold green]")
        display(result_top.toPandas())
        #Resultado detalhado do left join entre a tabela do banco com dicionário de dados. A partir da coluna (colunm_name)
        console.rule("[bold green]DETALHES DA TABELA E DICIONÁRIO [/bold green]")
        display(result_detalhado.toPandas().style.applymap(neg_vermelho, subset=['coluna_existente_no_dic?', 'tipagem_igual_dic?', 'null_igual_dic?']))

        self.spark.stop()
        
