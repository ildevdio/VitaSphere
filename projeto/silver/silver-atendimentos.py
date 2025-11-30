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

df_atendimentos = spark.sql("select * from classes.bronze.atendimentos")

# COMMAND ----------

#trocar nomes
column_mapping = {
    'atendimento_id': 'atendimento_id',
    'paciente_id': 'paciente_id',
    'nome_paciente': 'nome_paciente',
    'doenca': 'doenca',
    'localidade': 'localidade',
    'especialidade': 'especialidade',
    'local_atendimento': 'local_atendimento',
    'data_atendimento': 'data_atendimento',
    'data_consulta': 'data_consulta',
    'medico': 'medico'
}

for old_col, new_col in column_mapping.items():
    df_atendimentos = df_atendimentos.withColumnRenamed(old_col, new_col)

# COMMAND ----------

#cria tabela com a diferença entre os meses entre atendimento e consulta
df_atendimentos = df_atendimentos.withColumn(
    "diferença_atendimento_consulta",
    months_between(col("data_consulta"), col("data_atendimento"))
)

# COMMAND ----------

#transforma nova coluna em inteiro
df_atendimentos = df_atendimentos.withColumn("diferença_atendimento_consulta", col("diferença_atendimento_consulta").cast("int"))

# COMMAND ----------

#remover caracteres bugados
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StringType

for c, t in df_atendimentos.dtypes:
    if t == "string":
        df_atendimentos = df_atendimentos.withColumn(
            c,
            regexp_replace(col(c), r"[^\x20-\x7EÀ-ÿ]", "")
        )


# COMMAND ----------

#verificando duplicatas
df_atendimentos.groupBy(df_atendimentos.columns).count().filter("count > 1").show()

# COMMAND ----------

#remove duplicatas
df_atendimentos = df_atendimentos.dropDuplicates()

# COMMAND ----------

display(df_atendimentos)

# COMMAND ----------

df_atendimentos.write.saveAsTable(
    "classes.gold.df_atendimentos",
    mode="overwrite"
)
