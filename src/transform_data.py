import pandas as pd
from pathlib import Path
import json
import logging

logger = logging.getLogger(__name__)

path_name = Path('/opt/airflow/data/weather_data.json')
columns_names_to_drop = ['weather', 'base', 'weather_icon', 'sys.type']
columns_names_to_rename = {
    "base": "base",
        "visibility": "visibility",
        "dt": "datetime",
        "timezone": "timezone",
        "id": "city_id", 
        "name": "city_name",
        "cod": "code",
        "coord.lon": "longitude",
        "coord.lat": "latitude",
        "main.temp": "temperature",
        "main.feels_like": "feels_like",
        "main.temp_min": "temp_min",
        "main.temp_max": "temp_max",
        "main.pressure": "pressure",
        "main.humidity": "humidity",
        "main.sea_level": "sea_level",
        "main.grnd_level": "grnd_level",
        "wind.speed": "wind_speed",
        "wind.deg": "wind_deg",
        "wind.gust": "wind_gust",
        "clouds.all": "clouds", 
        "sys.type": "sys_type",                 
        "sys.id": "sys_id",                
        "sys.country": "country",                
        "sys.sunrise": "sunrise",                
        "sys.sunset": "sunset",
        # weather_id, weather_main, weather_description
}
columns_to_normalize_datetime = {'datetime', 'sunrise', 'sunset'}

def create_dataframe(path_name: str) -> pd.DataFrame:
    logger.info(f"Iniciando criação do dataframe a partir de: {path_name}")
    path = Path(path_name)

    if not path.exists():
        logger.error(f"Arquivo não encontrado: {path}")
        raise FileNotFoundError(f"O arquivo {path} não foi encontrado")

    with open(path, 'r') as f:
        data = json.load(f)

    df = pd.json_normalize(data)
    logger.info(f"Dataframe criado com sucesso — {len(df)} linhas, {len(df.columns)} colunas")
    return df

def normalize_weather_columns(df:pd.DataFrame) -> pd.DataFrame:
    df_weather = pd.json_normalize(df['weather'].apply(lambda x: x[0]))

    df_weather = df_weather.rename(columns={
        'id': 'weather_id',
        'main': 'weather_main',
        'description': 'weather_description',
        'icon': 'weather_icon'
    })

    df = pd.concat([df, df_weather], axis=1)
    logger.info("Colunas de weather normalizadas com sucesso")
    return df

def drop_columns(df: pd.DataFrame, columns_names: list[str]) -> pd.DataFrame:
    logger.info(f"Removendo colunas: {columns_names}")
    df = df.drop(columns=columns_names)
    logger.info(f"Colunas removidas — dataframe agora com {len(df.columns)} colunas")
    return df

def rename_columns(df: pd.DataFrame, columns_names: dict[str, str]) -> pd.DataFrame:
    logger.info(f"Renomeando {len(columns_names)} colunas")
    df = df.rename(columns=columns_names)
    logger.info("Colunas renomeadas com sucesso")
    return df

def normalize_datetime_columns(df: pd.DataFrame, columns_names: list[str]) -> pd.DataFrame:
    logger.info(f"Convertendo colunas para datetime: {columns_names}")
    for name in columns_names:
        df[name] = pd.to_datetime(df[name], unit='s', utc=True).dt.tz_convert('America/Sao_Paulo')
    logger.info("Colunas convertidas para datetime com sucesso")
    return df

def data_transformations():
    logger.info("=== Iniciando pipeline de transformações ===")
    df = create_dataframe(path_name)
    df = normalize_weather_columns(df)
    df = drop_columns(df, columns_names_to_drop)
    df = rename_columns(df, columns_names_to_rename)
    df = normalize_datetime_columns(df, columns_to_normalize_datetime)
    logger.info(f"=== Transformações concluídas — {len(df)} linhas, {len(df.columns)} colunas ===")
    return df