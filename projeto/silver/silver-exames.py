# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, year, lit, when, trim, regexp_replace,
    datediff, months_between
)
from pyspark.sql.types import (
    IntegerType, DoubleType, StringType, DateType
)

# COMMAND ----------

df_exames = spark.sql("select * from classes.bronze.exames")

# COMMAND ----------

column_mapping = {
  'exame_id': 'exame_id',
  'paciente_id': 'paciente_id',
  'nome_paciente': 'nome_paciente',
  'nome_exame': 'nome_exame',
  'data_realizacao': 'data_realizacao',
  'solicitante': 'solicitante',
  'localidade': 'localidade',
  'especialidade_solicitante': 'especialidade_solicitante'
}

for old_col, new_col in column_mapping.items():
    df_exames = df_exames.withColumnRenamed(old_col, new_col)

# COMMAND ----------

df_exames = df_exames.withColumn(
    'data_realizacao',
    to_date(df_exames['data_realizacao'], 'dd/MM/yyyy')
    )

# COMMAND ----------

#verificando duplicatas
df_exames.groupBy(df_exames.columns).count().filter("count > 1").show()

# COMMAND ----------

#remove duplicatas
df_exames = df_exames.dropDuplicates()


# COMMAND ----------

#remover caracteres bugados
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StringType

for c, t in df_exames.dtypes:
    if t == "string":
        df_exames = df_exames.withColumn(
            c,
            regexp_replace(col(c), r"[^\x20-\x7EÀ-ÿ]", "")
        )


# COMMAND ----------

display(df_exames)

# COMMAND ----------

df_exames.write.saveAsTable(
    "classes.gold.df_exames",
    mode="overwrite"
)
