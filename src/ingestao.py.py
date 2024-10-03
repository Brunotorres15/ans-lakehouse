# Databricks notebook source
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

table_exists(catalog, database, table)


# COMMAND ----------

df_full = (spark.read.format('parquet')
    .option("inferSchema", "true")
    .load('/Volumes/landing/upsell/operadoras/full/'))

df_full.coalesce(1).write.format('delta').saveAsTable(f'{catalog}.{database}.{table}')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from bronze.operadoras_full.operadoras
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


