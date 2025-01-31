# src/db/connection.py

import mysql.connector
from mysql.connector import errorcode
from src.config.settings import settings
from src.utils.logger import logger

def get_db_connection():
    try:
        cnx = mysql.connector.connect(
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            database=settings.DB_DATABASE,
            raise_on_warnings=True
        )
        logger.info("Conexión a la base de datos establecida exitosamente.")
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logger.error("Error: Credenciales de acceso denegadas.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logger.error("Error: La base de datos no existe.")
        else:
            logger.error(f"Error al conectar a la base de datos: {err}")
        raise
