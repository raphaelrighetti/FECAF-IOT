import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime

# Configuração do banco de dados
DATABASE_URL = "postgresql://postgres:root@localhost:5432/iot"

# Criar engine de conexão
engine = create_engine(DATABASE_URL)

# Criar base para os modelos
Base = declarative_base()

# Definir a tabela como uma classe ORM
class TemperatureLog(Base):
    __tablename__ = "temperature_logs"

    pk = Column(Integer, primary_key=True, autoincrement="auto")
    id = Column(String)
    room_id = Column(String)
    noted_date = Column(DateTime)
    temp = Column(Float)
    in_out = Column(String)

# Criar a tabela no banco de dados
views = [
    "avg_temp_por_ambiente",
    "contagem_por_faixa_temp",
    "temp_max_min_por_dia"
]

with engine.begin() as connection:
    for view in views:
        connection.execute(text(f"DROP VIEW IF EXISTS {view} CASCADE;"))
    print("* Views removidas com sucesso!")

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
print("* Tabela criada com sucesso!")

# Ler o CSV
df = pd.read_csv("IOT-temp.csv", parse_dates=["noted_date"], dayfirst=True)

# Renomear colunas
df.columns = ["id", "room_id", "noted_date", "temp", "in_out"]

# Criar sessão do SQLAlchemy
Session = sessionmaker(bind=engine)
session = Session()

# Converter DataFrame para lista de objetos ORM
registros = [TemperatureLog(**row) for row in df.to_dict(orient="records")]

# Inserir no banco
session.add_all(registros)
session.commit()
session.close()

print("* Dados importados com sucesso!")

# Definição das views
views_sql = [
    """
    CREATE OR REPLACE VIEW avg_temp_por_ambiente AS
    SELECT in_out, AVG(temp) AS avg_temp
    FROM temperature_logs
    GROUP BY in_out;
    """,
    """
    CREATE OR REPLACE VIEW contagem_por_faixa_temp AS
    SELECT in_out,
           COUNT(CASE WHEN temp < 20 THEN 1 END) AS abaixo_20,
           COUNT(CASE WHEN temp BETWEEN 20 AND 30 THEN 1 END) AS entre_20_30,
           COUNT(CASE WHEN temp > 30 THEN 1 END) AS acima_30
    FROM temperature_logs
    GROUP BY in_out;
    """,
    """
    CREATE OR REPLACE VIEW temp_max_min_por_dia AS
    SELECT DATE(noted_date) AS data,
           MAX(temp) AS temp_max,
           MIN(temp) AS temp_min
    FROM temperature_logs
    GROUP BY DATE(noted_date)
    ORDER BY data;
    """
]

# Criando as views no banco de dados
with engine.begin() as connection:
    try:
        for view_sql in views_sql:
            connection.execute(text(view_sql))
        print("* Views criadas com sucesso!")
    except Exception as e:
        print(f"x Erro ao criar views: {e}")
