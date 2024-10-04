# Databricks notebook source
def table_exists(catalog, database, table):
    count = (spark.sql(f"show tables from {catalog}.{database}").filter(f"database = '{database}' and tableName = '{table}'")).count()

    return count == 1

#table_exists(catalog, database, table)

