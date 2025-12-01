# Databricks notebook source
import pandas as pd
import plotly.express as px

# ---- LEITURA ----
df = spark.table("classes.gold.df_internacao")
pdf = df.toPandas()

# ---- AGRUPAMENTO ----
pdf_grouped = (
    pdf.groupby("procedimento_descricao")["quantidade_aprovada"]
    .sum()
    .reset_index()
)

# ---- PERCENTUAL ----
pdf_grouped["percentual"] = (
    pdf_grouped["quantidade_aprovada"] /
    pdf_grouped["quantidade_aprovada"].sum()
) * 100

pdf_grouped = pdf_grouped.sort_values("percentual", ascending=False)

# ---- SEPARA ≥ 1.50% E < 1.50% ----
acima_2 = pdf_grouped[pdf_grouped["percentual"] >= 1.50]
abaixo_2 = pdf_grouped[pdf_grouped["percentual"] < 1.50]

# Soma dos menores
outros_valor = abaixo_2["percentual"].sum()

# ---- MONTA DF FINAL (COM OUTROS) ----
df_final = acima_2[["procedimento_descricao", "percentual"]].copy()
df_final.loc[len(df_final)] = ["Outros", outros_valor]

# ================================================================
# 📌 GRÁFICO 1 — COM "OUTROS"
# ================================================================
fig1 = px.pie(
    df_final,
    names="procedimento_descricao",
    values="percentual",
    hole=0.55
)

fig1.update_traces(
    textposition="inside",
    textinfo="percent+label",
    marker=dict(line=dict(color="white", width=2))
)

fig1.update_layout(
    title={
        "text": "Distribuição Percentual — ≥ 1.50% + Outros",
        "x": 0.5,
        "font": dict(size=26)
    },
    width=1100,
    height=750,
    margin=dict(t=120, b=60, l=50, r=50)
)

fig1.show()


# ================================================================
# 📌 GRÁFICO 2 — SEM "OUTROS" (APENAS ≥ 1.50%)
# ================================================================
fig2 = px.pie(
    acima_2,
    names="procedimento_descricao",
    values="percentual",
    hole=0.55
)

fig2.update_traces(
    textposition="inside",
    textinfo="percent+label",
    marker=dict(line=dict(color="white", width=2))
)

fig2.update_layout(
    title={
        "text": "Distribuição Percentual — Apenas ≥ 1.50% (Sem Outros)",
        "x": 0.5,
        "font": dict(size=26)
    },
    width=1100,
    height=750,
    margin=dict(t=120, b=60, l=50, r=50)
)

fig2.show()

