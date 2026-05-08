import requests
import json
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def extract_weather_data(url: str) -> list:
    logger.info("Iniciando extração de dados meteorológicos")
    logger.info(f"URL: {url}")

    response = requests.get(url)
    logger.info(f"Status da resposta: {response.status_code}")

    if response.status_code != 200:
        logger.error(f"Erro na requisição: {response.status_code}")
        return []

    data = response.json()

    if not data:
        logger.warning(f"Não foi possível obter os dados: {data}")
        return []

    output_path = '/opt/airflow/data/weather_data.json'
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=4)

    logger.info(f"Dados salvos em {output_path} ({len(json.dumps(data))} bytes)")
    return data