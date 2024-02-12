from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
import pandas as pd

class DataFrameProcessor:
    def __init__(self, file_name, sheet_name, query_sqlserver, engine):
        self.spark = SparkSession.builder \
            .appName("DataFrame Processor") \
            .getOrCreate()
        self.file_name = file_name
        self.sheet_name = sheet_name
        self.query_sqlserver = query_sqlserver
        self.engine = engine

    # Realiza a leitura da planilha para o DataFrame   
    def read_excel_to_df(self):
        dic_vw_pd = pd.read_excel(self.file_name, sheet_name=self.sheet_name)
        self.dic_vw = self.spark.createDataFrame(dic_vw_pd)
        
    # Consulta SQL para selecionar os metadados da tabela
    def execute_sql_to_df(self):
        df_pd = pd.read_sql(self.query_sqlserver, self.engine)
        self.df = self.spark.createDataFrame(df_pd)  
    
    # Defina as condições para criar a nova coluna dtype_consolidado, consolida tipos de dados semelhantes como algo único para validação posteriormente    
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
        

        # Realize o left join da tabela do banco com dicionário, join pela coluna "column_name"
        result = self.df.join(
            self.dic_vw, self.df["column_name"] == self.dic_vw["column_name"], "left") \
            .select(self.df["*"], self.dic_vw["column_name"].alias('dic_column_name'),
            self.dic_vw["data_type"].alias('dic_data_type'), self.dic_vw["is_nullable"].alias('dic_is_nullable'))
        
        
        # Colunas não existente na tabela mas estão no dicionário, join pela coluna "column_name"
        result2 = self.dic_vw.join(
            self.df, self.dic_vw["column_name"] == self.df["column_name"], "left_outer") \
            .filter(self.df["column_name"].isNull()) \
            .select(self.dic_vw["column_name"], self.dic_vw["data_type"], self.dic_vw["is_nullable"])

        
        # Adiciona as colunas adicionais com lógica condicional, comparando as colunas, tipagem e nulls com as mesmas infos do dicionário.        
        result = result.withColumn(
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
        result_consolidado = result.withColumn(
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
        

        print('---------------------------------------------------------------------- RESUMO DA TABELA DO BANCO ----------------------------------------------------------------------')
        result_consolidado.show(truncate=False)
        print('---------------------------------------------------------------------- DETALHES DA TABELA E DICIONÁRIO ----------------------------------------------------------------------')
        result.show(truncate=False)
        print('------------------------------------------------- COLUNAS DO DICIONÁRIO NÃO ENCONTRADAS NA TABELA DO BANCO -------------------------------------------------')
        result2.show(truncate=False)
        
        #print('------------------------------------------------------------------------ TABELA DO BANCO ------------------------------------------------------------------------')
        #self.df.show(truncate=False)
        
        #print('------------------------------------------------------------------------ TABELA DO DICIONÁRIO ------------------------------------------------------------------------')
        #self.dic_vw.show(truncate=False)

    def close_session(self):
        self.spark.stop()
