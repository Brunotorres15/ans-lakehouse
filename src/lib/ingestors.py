# Databricks notebook source
class Ingestor:
    def __init__(self, spark, catalog, database, table):
        self.spark = spark
        self.catalog = catalog
        self.database = database
        self.table = table

    def load_full(self, path):
        df_full = (spark.read.format('parquet')
        .option("inferSchema", "true")
        .load(path))

        return df_full
    
    def save_full(self, df):
        df.coalesce(1).write.format('delta').mode('overwrite').saveAsTable(f'{catalog}.{database}.{table}')

        return True
    
    def execute_full_load(self, path):
        df = self.load_full(path)

        return self.save_full(df)

    
    



