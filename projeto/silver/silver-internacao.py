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

df_internacao = spark.sql("select * from classes.bronze.internacao")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df_internacao = df_internacao

for c in df_internacao.columns:
    df_internacao = df_internacao.withColumn(c, regexp_replace(col(c), '"', ''))

# COMMAND ----------

#muda os nomes das colunas
column_mapping = {
    '"""Procedimento""': 'procedimento',
    '""Quantidade aprovada""': 'quantidade_aprovada',
    '""Valor aprovado"""': 'valor_aprovado'
}
for old_col, new_col in column_mapping.items():
    df_internacao = df_internacao.withColumnRenamed(old_col, new_col)

# COMMAND ----------

df_internacao = df_internacao.withColumn(
    "quantidade_aprovada",
    regexp_replace(col("quantidade_aprovada"), r"[^0-9]", "")  # remove tudo que não é número
)


# COMMAND ----------

df_internacao = df_internacao.withColumn(
    "valor_aprovado",
    regexp_replace(col("valor_aprovado"), ",", ".")   # troca vírgula por ponto
)

# COMMAND ----------

df_internacao = df_internacao.withColumn(
    "valor_aprovado",
    regexp_replace(col("valor_aprovado"), r"[^0-9.]", "") # remove tudo que não é número e .
)

# COMMAND ----------


df_internacao = df_internacao.withColumn(
    "quantidade_aprovada",
    when(col("quantidade_aprovada") == "", "0")                # se ficou vazio, vira 0
    .otherwise(col("quantidade_aprovada"))
)



# COMMAND ----------

df_internacao = df_internacao.withColumn(
    "valor_aprovado",
    when(col("valor_aprovado") == "", "0")                # se ficou vazio, vira 0
    .otherwise(col("valor_aprovado"))
)

# COMMAND ----------

#transforma coluna de quantidade_aprovada em bigint
df_internacao = df_internacao.withColumn(
    "quantidade_aprovada",
    regexp_replace(col("quantidade_aprovada"), r"[^0-9]", "").cast("bigint")
)

# COMMAND ----------

from pyspark.sql.functions import round, col

#transforma coluna de valor_aprovado em double
df_internacao = df_internacao.withColumn(
    "valor_aprovado",
    round(col("valor_aprovado"), 2)
)

# COMMAND ----------


#extrai id e descricao do procedimento
from pyspark.sql.functions import regexp_extract

df_internacao = df_internacao.withColumn("procedimento_id", regexp_extract("procedimento", r"^(\d+)", 1)) \
       .withColumn("procedimento_descricao", regexp_extract("procedimento", r"^\d+\s*(.*)$", 1))
df_internacao = df_internacao.drop("procedimento")

# COMMAND ----------

#muda a ordem de exibição das colunas
df_internacao = df_internacao.select("procedimento_id", "procedimento_descricao", "quantidade_aprovada", "valor_aprovado")

# COMMAND ----------

#tira caracteres especiais de procedimento_descrição
from pyspark.sql.functions import regexp_replace, col

df_internacao = df_internacao.withColumn(
    "procedimento_descricao",
    regexp_replace(col("procedimento_descricao"), r"[^\x20-\x7EÀ-ÿ]", "")
)

# COMMAND ----------

#verificando duplicatas
df_internacao.groupBy(df_internacao.columns).count().filter("count > 1").show()

# COMMAND ----------

#remove duplicatas
df_internacao = df_internacao.dropDuplicates()

# COMMAND ----------

#remover caracteres bugados
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import StringType

for c, t in df_internacao.dtypes:
    if t == "string":
        df_internacao = df_internacao.withColumn(
            c,
            regexp_replace(col(c), r"[^\x20-\x7EÀ-ÿ]", "")
        )


# COMMAND ----------

display(df_internacao)

# COMMAND ----------

from pyspark.sql import functions as F
from functools import reduce

# Remove linhas com NULL
df_internacao = df_internacao.na.drop("any")

# Remove strings vazias em qualquer coluna
condicoes = [
    (F.col(c).isNotNull()) & (F.trim(F.col(c)) != "")
    for c in df_internacao.columns
]

df_internacao = df_internacao.filter(
    reduce(lambda a, b: a & b, condicoes)
)

# COMMAND ----------

df_internacao.write.saveAsTable(
    "classes.gold.df_internacao",
    mode="overwrite"
)
