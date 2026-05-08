from sqlalchemy import create_engine
from urllib.parse import quote_plus
import os
import pandas as pd
import logging

logger = logging.getLogger(__name__)

user = os.getenv('user')
password = os.getenv('password')
database = os.getenv('database')
host = os.getenv('host', 'postgres')

engine = None

def _ensure_engine():
    global engine
    if engine is None:
        logger.info(f"Conectando ao banco {database} em {host}:5432")
        engine = create_engine(
            f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:5432/{database}"
        )
        logger.info("Conexão com banco estabelecida")
    return engine

def load_weather_data(table_name: str, df):
    logger.info(f"Iniciando carga de {len(df)} registros na tabela '{table_name}'")
    df.to_sql(
        name=table_name,
        con=_ensure_engine(),
        if_exists='append',
        index=False
    )
    logger.info(f"Dados carregados com sucesso na tabela '{table_name}'")

    df_check = pd.read_sql(f'SELECT COUNT(*) as total FROM {table_name}', con=_ensure_engine())
    total = df_check['total'].iloc[0]
    logger.info(f"Total de registros na tabela '{table_name}': {total}")