# Databricks notebook source
import plotly.express as px
import pandas as pd

# ---- 1) Carrega a tabela da camada gold ----
df = classes.gold.df_internacao

# ---- 2) Configurações ----
limite_procedimentos = 5  # quantos aparecem individualmente

def preparar_df(df, coluna_valor):
    df_sorted = df.sort_values(coluna_valor, ascending=False)
    df_top = df_sorted.head(limite_procedimentos)
    df_rest = df_sorted.iloc[limite_procedimentos:]

    outros_valor = df_rest[coluna_valor].sum()

    df_final = df_top.copy()
    df_final.loc[len(df_final)] = [
        "OUTROS",
        "OUTROS",
        outros_valor,
        outros_valor,
    ]

    df_final["percentual"] = df_final[coluna_valor] / df_final[coluna_valor].sum() * 100
    return df_final

# ---- 3) Cria figuras para cada métrica ----
metricas = {
    "Quantidade": "quantidade_aprovada",
    "Valor": "valor_aprovado"
}

figs = {}
for nome, coluna in metricas.items():
    df_plot = preparar_df(df, coluna)
    fig = px.pie(
        df_plot,
        names='procedimento_descricao',
        values='percentual',
        title=f"Distribuição por {nome}",
        hole=0.4
    )

    fig.update_layout(
        height=600,
        width=850,
        title_font_size=26,
        legend_title_text="Procedimentos",
        legend=dict(font=dict(size=14)),
        margin=dict(l=40, r=40, t=80, b=40)
    )
    figs[nome] = fig

# ---- 4) Cria dropdown no gráfico principal ----
fig_final = figs["Quantidade"]  # default
fig_final.update_layout(
    updatemenus=[
        {
            "buttons": [
                {
                    "label": nome,
                    "method": "restyle",
                    "args": [
                        {"values": [preparar_df(df, coluna)["percentual"]], 
                         "labels": [preparar_df(df, coluna)["procedimento_descricao"]]}
                    ],
                }
                for nome, coluna in metricas.items()
            ],
            "direction": "down",
            "showactive": True,
            "x": 1.15,
            "y": 1.0,
        }
    ]
)

fig_final.show()


# COMMAND ----------

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

# ---- SEPARA ≥ 2% E < 2% ----
acima_2 = pdf_grouped[pdf_grouped["percentual"] >= 1.50]
abaixo_2 = pdf_grouped[pdf_grouped["percentual"] < 1.50]

# Soma dos menores
outros_valor = abaixo_2["percentual"].sum()

# ---- MONTA DF FINAL ----
df_final = acima_2[["procedimento_descricao", "percentual"]].copy()
df_final.loc[len(df_final)] = ["Outros", outros_valor]
df_outros = outros_valor
df_final["percentual"] = df_final["percentual"] + df_outros

# ---- GRÁFICO BONITO ----
fig = px.pie(
    df_final,
    names="procedimento_descricao",
    values="percentual",
    hole=0.55
)

fig.update_traces(
    textposition="inside",
    textinfo="percent+label",
    marker=dict(line=dict(color="white", width=2))
)

fig.update_layout(
    title={
        "text": "Procedimentos de Internação — Distribuição Percentual (≥ 2% + Outros)",
        "x": 0.5,
        "y": 0.96,
        "font": dict(size=26)
    },
    width=1100,
    height=750,
    margin=dict(t=130, b=60, l=50, r=50)
)

fig.show()

