# src/api/client.py

import requests
from requests.auth import HTTPBasicAuth
import json
from src.config.settings import settings
from src.utils.logger import logger

class APIClient:
    def __init__(self):
        self.url = settings.API_URL
        self.auth = HTTPBasicAuth(settings.API_USERNAME, settings.API_PASSWORD)
    
    def get_data(self, payload):
        try:
            response = requests.get(
                self.url,
                auth=self.auth,
                json=payload,
                verify=True  # Es recomendable manejar certificados correctamente
            )
            if response.status_code == 200:
                try:
                    data = response.json()
                    logger.info("Datos obtenidos exitosamente desde la API.")
                    return data
                except json.JSONDecodeError:
                    logger.error("La respuesta de la API no está en formato JSON.")
                    return None
            else:
                logger.error(f"Error en la solicitud a la API: {response.status_code} - {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Ocurrió un error al realizar la solicitud a la API: {e}")
            return None
