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

df_imunizacoes = spark.sql("select * from classes.bronze.imunizacoes")

# COMMAND ----------

column_mapping = {
    "Unidade da Federa��o": "unidade_da_federacao",
    "BCG": "bcg",
    "Hepatite B idade <= 30 dias": "hepatite_b_idade_menor_igual_30_dias",
    "Rotav�rus Humano": "rotavirus_humano",
    "Meningococo C": "meningococo_c",
    "Hepatite B": "hepatite_b",
    "Penta": "penta",
    "Pneumoc�cica": "pneumococica",
    "Poliomielite": "poliomielite",
    "Poliomielite 4 anos": "poliomielite_4_anos",
    "Febre Amarela": "febre_amarela",
    "Hepatite A": "hepatite_a",
    "Pneumoc�cica(1� ref)": "pneumococica_1_ref",
    "Meningococo C (1� ref)": "meningococo_c_1_ref",
    "Poliomielite(1� ref)": "poliomielite_1_ref",
    'Triplice Viral D1': 'triplice_viral_d1',
    'Triplice Viral D2': 'triplice_viral_d2',
    "Tetra Viral(SRC+VZ)": "tetra_viral_src_vz",
    "DTP": "dtp",
    "DTP REF (4 e 6 anos)": "dtp_ref_4_6_anos",
    "Tr�plice Bacte(DTP)(1� ref)": "triplice_bacte_dtp_1_ref",
    "Sarampo": "sarampo",
    "Haemophilus influenzae b": "haemophilus_influenzae_b",
    "Dupla adulto e dTpa gestante": "dupla_adulto_dtpa_gestante",
    "dTpa gestante": "dtpa_gestante",
    "Tetravalente(DTP/Hib)(TETRA)": "tetravalente_dtp_hib_tetra",
    "Varicela": "varicela"
}
for old_col, new_col in column_mapping.items():
    df_imunizacoes = df_imunizacoes.withColumnRenamed(old_col, new_col)

# COMMAND ----------

from pyspark.sql.functions import col

novos_nomes = {}
for c in df_imunizacoes.columns:
    if "�" in c:
        novos_nomes[c] = c.replace("�", "i")

for velho, novo in novos_nomes.items():
    df_imunizacoes = df_imunizacoes.withColumnRenamed(velho, novo)


# COMMAND ----------

#extrai id e descricao do procedimento
from pyspark.sql.functions import regexp_extract

df_imunizacoes = df_imunizacoes.withColumn("numero_da_federacao", regexp_extract("unidade_da_federacao", r"^(\d+)", 1)) \
       .withColumn("federacao", regexp_extract("unidade_da_federacao", r"^\d+\s*(.*)$", 1))
df_imunizacoes = df_imunizacoes.drop("unidade_da_federacao")

# COMMAND ----------

#ordenar as colunas
primeiras = [
    "numero_da_federacao",
    "federacao",
]

resto = [c for c in df_imunizacoes.columns if c not in primeiras]

df_imunizacoes = df_imunizacoes.select(*(primeiras + resto))


# COMMAND ----------

primeiras = [
    "numero_da_federacao",
    "federacao",
]

resto = [c for c in df_imunizacoes.columns if c not in primeiras]

from pyspark.sql.functions import regexp_replace, col

for c in resto:
    df_imunizacoes = df_imunizacoes.withColumn(
        c,
        regexp_replace(col(c), ",", ".").cast("double")
    )

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, when, col, lit
import pyspark.sql.functions as F

# cria um índice para achar a última linha
w = Window.orderBy(F.monotonically_increasing_id())

df_imunizacoes = df_imunizacoes.withColumn(
    "rn", 
    row_number().over(w)
)

ultima = df_imunizacoes.agg({"rn": "max"}).collect()[0][0]

# define federacao como NULL na última linha
df_imunizacoes = df_imunizacoes.withColumn(
    "federacao",
    when(col("rn") == ultima, lit(None)).otherwise(col("federacao"))
).drop("rn")


# COMMAND ----------

from pyspark.sql.functions import col, when

df_imunizacoes = df_imunizacoes.withColumn(
    "federacao",
    when(col("federacao").isNull(), "TOTAL")
    .otherwise(col("federacao"))
)

# COMMAND ----------

correcoes = {
    "Rond�nia": "rondonia",
    "Par�": "para",
    "Paran�": "parana",
    "Amap�": "amapa",
    "Maranh�o": "maranhao",
    "Cear�": "ceara",
    "Piau�": "piaui",
    "Para�ba": "paraiba",
    "Goi�s": "goias",
    "Esp�rito Santo": "espirito Santo",
    "S�o Paulo": "sao paulo"
}

for errado, correto in correcoes.items():
    df_imunizacoes = df_imunizacoes.withColumn(
        "federacao",
        when(col("federacao") == errado, correto)
        .otherwise(col("federacao"))
    )

# COMMAND ----------

from pyspark.sql.functions import lower, col

df_imunizacoes = df_imunizacoes.withColumn(
    "federacao",
    lower(col("federacao"))
)

# COMMAND ----------

import unicodedata
import re

def normalizar_coluna(nome):
    # remove acentos
    nome = unicodedata.normalize('NFKD', nome).encode('ASCII', 'ignore').decode('ASCII')

    # coloca tudo minúsculo
    nome = nome.lower()

    # remove qualquer coisa que não seja letra, número ou espaço
    nome = re.sub(r'[^a-z0-9 ]', ' ', nome)

    # troca espaços múltiplos por 1 só underline
    nome = re.sub(r'\s+', '_', nome)

    # remove underlines duplicados do começo ou final
    nome = nome.strip('_')

    return nome

# aplica para todas as colunas
novos_nomes = [normalizar_coluna(c) for c in df_imunizacoes.columns]
df_imunizacoes = df_imunizacoes.toDF(*novos_nomes)


# COMMAND ----------


display(df_imunizacoes)

# COMMAND ----------

df_imunizacoes.write.saveAsTable(
    "classes.gold.df_imunizacoes",
    mode="overwrite"
)
