# Databricks notebook source
import delta
import sys

sys.path.insert(0, "../lib/")

import utils
import ingestors

# Definir a configuração do parâmetro
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

# COMMAND ----------

catalog = 'bronze'
database = 'operadoras_full'
table = 'operadoras'
path_full = '/Volumes/landing/upsell/operadoras/full/'

# COMMAND ----------

if not utils.table_exists(catalog, database, table):

    print('Tabela não existente, criando...')

    ingest_full_load = ingestors.Ingestor(spark = spark,
                                          catalog = catalog,
                                          database = database,
                                          table = table)
    
    ingest_full_load.execute_full_load(path_full)
    print("Carga full-load realizada com sucesso!")

else:
    print("Tabela já existe, realizando CDC...")

# COMMAND ----------



# COMMAND ----------


    df_cdc = (spark.read.format('parquet')
        .option("inferSchema", "true")
        .load('/Volumes/landing/upsell/operadoras/cdc/')).createGlobalTempView('operadoras_cdc')
    
    bronze = delta.DeltaTable.forName(spark, f'{catalog}.{database}.{table}')

    bronze.alias("t") \
        .merge(
            df_cdc.alias("s"),  # Fonte dos novos dados
            """
            target.ID_CMPT_MOVEL = source.ID_CMPT_MOVEL AND
            target.CD_OPERADORA = source.CD_OPERADORA AND
            target.NR_CNPJ = source.NR_CNPJ AND
            target.SG_UF = source.SG_UF AND
            target.CD_MUNICIPIO = source.CD_MUNICIPIO AND
            target.TP_SEXO = source.TP_SEXO AND
            target.DE_FAIXA_ETARIA = source.DE_FAIXA_ETARIA AND
            target.DE_FAIXA_ETARIA_REAJ = source.DE_FAIXA_ETARIA_REAJ AND
            target.CD_PLANO = source.CD_PLANO AND
            target.TIPO_VINCULO = source.TIPO_VINCULO
            """  # Condição de match (com base no id)
        ) \
        #.whenMatchedDelete()  # Quando houver match e a flag is_deleted for true, faz o delete
        .whenMatchedUpdateAll()  # Quando houver match, faz o update de todas as colunas
        .whenNotMatchedInsertAll()  # Quando não houver match, insere todos os dados novos
        .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.operadoras_full.operadoras
# MAGIC

# COMMAND ----------

df_cdc = (spark.read.format('parquet')
        .option("inferSchema", "true")
        .load('/Volumes/landing/upsell/operadoras/cdc/'))
        
df_cdc.createOrReplaceGlobalTempView(f'view_operadoras')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.view_operadoras

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


