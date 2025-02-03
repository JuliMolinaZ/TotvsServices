from src.db.connection import get_db_connection
from src.db.operations import insertar_picklist, insertar_picklist_detalle
from src.utils.logger import logger
from src.utils.helpers import validate_data

class DataService:
    """Clase para manejar la inserción de datos en la base de datos."""

    def __init__(self):
        self.cnx = None
        self.cursor = None

    def conectar_bd(self):
        """Conecta a la base de datos y crea el cursor."""
        try:
            self.cnx = get_db_connection()
            self.cursor = self.cnx.cursor()
            logger.info("Conexión a la base de datos establecida")
        except Exception as e:
            logger.error(f"No se pudo establecer conexión con la base de datos: {e}")
            raise e

    def limpiar_tablas(self):
        """Borra los datos de las tablas antes de la inserción."""
        try:
            self.cursor.execute("DELETE FROM PickListTest")
            self.cursor.execute("DELETE FROM PickListDetalleTest")
            logger.info("⚠️ Datos previos eliminados de PickListTest y PickListDetalleTest.")
        except Exception as e:
            logger.error(f"Error al eliminar datos: {e}")

    def insertar_datos(self, datos):
        """Inserta los datos obtenidos de la API en la base de datos."""
        try:
            if not self.cnx.in_transaction:
                self.cnx.start_transaction()
                logger.info("Transacción iniciada.")

            for registro in datos:
                if not validate_data(registro):
                    logger.warning(f"Registro inválido omitido: {registro}")
                    continue

                picklist_id = insertar_picklist(self.cursor, registro)
                insertar_picklist_detalle(self.cursor, picklist_id, registro)

            self.cnx.commit()
            logger.info("Todos los datos han sido insertados correctamente.")

        except Exception as e:
            self.cnx.rollback()
            logger.error(f"Error durante la inserción: {e}")

    def cerrar_conexion(self):
        """Cierra la conexión con la base de datos."""
        if self.cursor:
            self.cursor.close()
        if self.cnx:
            self.cnx.close()
        logger.info("Conexión a la base de datos cerrada.")
