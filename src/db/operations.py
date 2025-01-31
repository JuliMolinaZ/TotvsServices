# src/db/operations.py

from src.utils.logger import logger
import mysql.connector

def insertar_picklist(cursor, data):
    """
    Inserta un registro en la tabla PickListTest.
    Retorna el PickListID generado.
    """
    try:
        add_picklist = (
            """
            INSERT INTO PickListTest (ClienteID, Deposito, Pedido, ClienteNombre, Tienda)
            VALUES (%s, %s, %s, %s, %s)
            """
        )
        picklist_data = (
            data['cliente'],          # Acceso directo para lanzar KeyError si falta
            data['deposito'],         # Acceso directo para lanzar KeyError si falta
            data['pedido'],           # Acceso directo para lanzar KeyError si falta
            data['nombre'],           # Acceso directo para lanzar KeyError si falta
            data['tienda']            # Acceso directo para lanzar KeyError si falta
        )
        cursor.execute(add_picklist, picklist_data)
        picklist_id = cursor.lastrowid
        logger.info(f"Insertado PickListTest con PickListID: {picklist_id}")
        return picklist_id
    except KeyError as e:
        logger.error(f"Campo faltante: {e}")
        raise
    except mysql.connector.Error as err:
        logger.error(f"Error al insertar en PickListTest: {err}")
        raise

def insertar_picklist_detalle(cursor, picklist_id, data):
    """
    Inserta un registro en la tabla PickListDetalleTest.
    """
    add_detalle = (
        """
        INSERT INTO PickListDetalleTest (PickListID, ProductoID, ProductoDescripcion, CantidadLiberada, Ubicacion)
        VALUES (%s, %s, %s, %s, %s)
        """
    )
    detalle_data = (
        picklist_id,
        data.get('producto'),
        data.get('descripcion'),
        data.get('cantidad_liberada'),
        data.get('ubicacion') if data.get('ubicacion') else None
    )
    try:
        cursor.execute(add_detalle, detalle_data)
        detalle_id = cursor.lastrowid
        logger.info(f"Insertado PickListDetalleTest con PickListDetalleID: {detalle_id}")
    except mysql.connector.Error as err:
        logger.error(f"Error al insertar en PickListDetalleTest: {err}")
        raise

