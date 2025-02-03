from src.services.data_service import DataService
from src.api.api_services import APIService
from src.utils.logger import setup_logger, logger

def main():
    """Función principal que orquesta la ejecución del programa."""
    setup_logger()
    logger.info("Iniciando ejecución del programa...")

   
    api_service = APIService()
    datos = api_service.obtener_datos()

    if not datos:
        return  

    data_service = DataService()

    logger.info(datos)
    try:
        data_service.conectar_bd()
        data_service.limpiar_tablas()
        data_service.insertar_datos(datos)
    except Exception as e:
        logger.error(f"Error al procesar datos: {e}")
    finally:
        data_service.cerrar_conexion()

if __name__ == "__main__":
    main()
