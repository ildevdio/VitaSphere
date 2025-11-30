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

df_cirurgias = spark.sql("select * from classes.bronze.cirurgias_sus")

# COMMAND ----------

column_mapping = {
    'nome_paciente': 'nome_paciente',
    'data_cirurgia': 'data_cirurgia',
    'cod_paciente': 'paciente_id',
    'tipo_cirurgia': 'tipo_cirurgia',
    'doenca': 'doenca',
    'hospital': 'nome_hospital',
    'tipo_procedimento': 'tipo_procedimento',
    'nome_medico': 'nome_medico',
    'resultado_cirurgia': 'resultado_cirurgia'
}
for old, new in column_mapping.items():
    df_cirurgias = df_cirurgias.withColumnRenamed(old, new)

# COMMAND ----------

#verificando duplicatas
df_cirurgias.groupBy(df_cirurgias.columns).count().filter("count > 1").show()

# COMMAND ----------

#remove duplicatas
df_cirurgias = df_cirurgias.dropDuplicates()

# COMMAND ----------

#remover caracteres bugados
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StringType

for c, t in df_cirurgias.dtypes:
    if t == "string":
        df_cirurgias = df_cirurgias.withColumn(
            c,
            regexp_replace(col(c), r"[^\x20-\x7EÀ-ÿ]", "")
        )


# COMMAND ----------

display(df_cirurgias)


# COMMAND ----------

df_cirurgias.write.saveAsTable(
    "classes.gold.df_cirurgias",
    mode="overwrite"
)
