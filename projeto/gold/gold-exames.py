{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "inputWidgets": {},
     "nuid": "e834f5a6-a2b5-4831-8962-7df3ea271df4",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import plotly.graph_objects as go\n",
    "from plotly.subplots import make_subplots\n",
    "\n",
    "df = spark.sql(\"SELECT * FROM classes.gold.df_exames\").toPandas()\n",
    "\n",
    "df['data_realizacao'] = pd.to_datetime(df['data_realizacao'])\n",
    "df['mes_ano'] = df['data_realizacao'].dt.to_period('M').astype(str)\n",
    "\n",
    "monthly = df.groupby('mes_ano').size().reset_index(name='quantidade')\n",
    "monthly['mes_ano'] = pd.to_datetime(monthly['mes_ano'])\n",
    "\n",
    "top_exames = df['nome_exame'].value_counts().head(15)\n",
    "top_especialidade = df['especialidade_solicitante'].value_counts().head(15)\n",
    "\n",
    "fig = make_subplots(\n",
    "    rows=1, cols=3,\n",
    "    subplot_titles=(\n",
    "        \"Evolução Mensal de Exames\",\n",
    "        \"Top 15 Exames Mais Realizados\",\n",
    "        \"Top 15 Especialidades Solicitantes\"\n",
    "    ),\n",
    "    column_widths=[0.40, 0.30, 0.30],\n",
    "    specs=[[{}, {\"type\": \"bar\"}, {\"type\": \"bar\"}]]\n",
    ")\n",
    "\n",
    "fig.add_trace(\n",
    "    go.Scatter(x=monthly['mes_ano'], y=monthly['quantidade'],\n",
    "               mode='lines+markers', line=dict(color='#2E91E5', width=3),\n",
    "               marker=dict(size=6), name='Exames por mês'),\n",
    "    row=1, col=1\n",
    ")\n",
    "\n",
    "fig.add_trace(\n",
    "    go.Bar(x=top_exames.values, y=top_exames.index, orientation='h',\n",
    "           marker_color='#E24A33', showlegend=False),\n",
    "    row=1, col=2\n",
    ")\n",
    "\n",
    "fig.add_trace(\n",
    "    go.Bar(x=top_especialidade.values, y=top_especialidade.index, orientation='h',\n",
    "           marker_color='#0B6623', showlegend=False),\n",
    "    row=1, col=3\n",
    ")\n",
    "\n",
    "fig.update_layout(\n",
    "    height=600,\n",
    "    title_text=\"Análise de Exames Médicos (2020–2025)\",\n",
    "    title_x=0.5,\n",
    "    showlegend=False,\n",
    "    font=dict(size=12),\n",
    "    margin=dict(l=60, r=60, t=90, b=60)\n",
    ")\n",
    "\n",
    "fig.update_xaxes(title_text=\"Data\", row=1, col=1, tickangle=45)\n",
    "fig.update_yaxes(title_text=\"Número de Exames\", row=1, col=1)\n",
    "fig.update_xaxes(title_text=\"Quantidade\", row=1, col=2)\n",
    "fig.update_xaxes(title_text=\"Quantidade\", row=1, col=3)\n",
    "\n",
    "fig.show()"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "4"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "gold-exames",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
