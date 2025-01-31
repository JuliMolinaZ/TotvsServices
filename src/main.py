# src/main.py

from src.api.client import APIClient
from src.db.connection import get_db_connection
from src.db.operations import insertar_picklist, insertar_picklist_detalle
from src.utils.logger import setup_logger, logger
from src.utils.helpers import validate_data

def procesar_datos(datos, cursor, cnx):
    """
    Procesa los datos obtenidos de la API e inserta en la base de datos.
    """
    try:
        # Iniciar una transacción
        cnx.start_transaction()
        logger.info("Transacción iniciada.")

        for registro in datos:
            # Validar datos antes de insertar
            if not validate_data(registro):
                logger.warning(f"Registro inválido omitido: {registro}")
                continue

            # Insertar en PickListTest
            picklist_id = insertar_picklist(cursor, registro)
            
            # Insertar en PickListDetalleTest
            insertar_picklist_detalle(cursor, picklist_id, registro)
        
        # Confirmar la transacción
        cnx.commit()
        logger.info("Todos los datos se han insertado exitosamente y la transacción ha sido confirmada.")
    
    except Exception as e:
        # Revertir la transacción en caso de error
        cnx.rollback()
        logger.error(f"Error durante la inserción de datos. La transacción ha sido revertida. Detalles: {e}")

def main():
    # Configurar el logger
    setup_logger()

    # Obtener datos desde la API
    api_client = APIClient()
    payload = {
        "referencia_serie": "20230719.......",
        "referencia_folio": "12:04:29......."
    }
    datos = api_client.get_data(payload)

    if not datos:
        logger.error("No se obtuvieron datos de la API. Terminando el proceso.")
        return

    # Conectar a la base de datos
    try:
        cnx = get_db_connection()
        cursor = cnx.cursor()
    except Exception as e:
        logger.error(f"No se pudo establecer conexión con la base de datos: {e}")
        return

    try:
        # Procesar e insertar los datos
        procesar_datos(datos, cursor, cnx)
    
    finally:
        # Cerrar el cursor y la conexión
        cursor.close()
        cnx.close()
        logger.info("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    main()
