# Databricks notebook source
import delta

# Definir a configuração do parâmetro
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

# COMMAND ----------

catalog = 'bronze'
database = 'operadoras_full'
table = 'operadoras'

# COMMAND ----------

def table_exists(catalog, database, table):
    count = (spark.sql(f"show tables from {catalog}.{database}").filter(f"database = '{database}' and tableName = '{table}'")).count()

    return count == 1

#table_exists(catalog, database, table)


# COMMAND ----------

if not table_exists(catalog, database, table):

    print('Tabela não existente, construindo...')

    df_full = (spark.read.format('parquet')
        .option("inferSchema", "true")
        .load('/Volumes/landing/upsell/operadoras/full/'))

    df_full.coalesce(1).write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{database}.{table}')
else:
    print('tabela já existente, realizando CDC...')

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


