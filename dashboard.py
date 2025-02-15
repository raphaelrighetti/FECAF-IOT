import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:root@localhost:5432/iot"

# Conexão com o banco de dados
engine = create_engine(DATABASE_URL)

# Função para carregar dados de uma view
def load_data(view_name):
  return pd.read_sql(f"SELECT * FROM {view_name}", engine)

# Título do dashboard
st.title('Dashboard de Temperaturas IoT')

# Gráfico 1: Média de temperatura por ambiente (In/Out)
st.header('Média de Temperatura por Ambiente')
df_avg_temp = load_data('avg_temp_por_ambiente')
fig1 = px.bar(df_avg_temp, x='in_out', y='avg_temp', title="Média de Temperatura por Ambiente")
st.plotly_chart(fig1)

# Gráfico 2: Contagem de registros por faixa de temperatura
st.header('Distribuição de Temperaturas por Ambiente')
df_contagem_temp = load_data('contagem_por_faixa_temp')
fig2 = px.bar(df_contagem_temp, x='in_out', y=['abaixo_20', 'entre_20_30', 'acima_30'],
              title="Contagem de Registros por Faixa de Temperatura",
              labels={'value': 'Quantidade', 'variable': 'Faixa de Temperatura'})
st.plotly_chart(fig2)

# Gráfico 3: Temperaturas máximas e mínimas por dia
st.header('Temperaturas Máximas e Mínimas por Dia')
df_temp_max_min = load_data('temp_max_min_por_dia')
fig3 = px.line(df_temp_max_min, x='data', y=['temp_max', 'temp_min'],
               title="Variação de Temperaturas por Dia",
               labels={'data': 'Data', 'value': 'Temperatura (°C)', 'variable': 'Tipo'})
st.plotly_chart(fig3)