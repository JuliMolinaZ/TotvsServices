from src.api.client import APIClient
from src.utils.logger import logger

class APIService:
    """Clase para manejar las llamadas a la API."""

    def __init__(self):
        self.api_client = APIClient()

    def obtener_datos(self):
        """Obtiene datos de la API con un payload específico."""
        payload = {
            "referencia_serie": "20230719.......",
            "referencia_folio": "12:04:29......."
        }

        logger.info("Enviando solicitud a la API...")
        datos = self.api_client.get_data(payload)

        if datos:
            logger.info(f"{len(datos)} registros recibidos de la API.")
        else:
            logger.error("No se obtuvieron datos de la API.")

        return datos
