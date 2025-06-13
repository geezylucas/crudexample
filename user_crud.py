#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class UserCRUD:
    """
    Clase para realizar operaciones CRUD en la tabla 'users'
    Asume que la tabla ya existe con la estructura:
    - id (SERIAL PRIMARY KEY)
    - nombre (VARCHAR)
    - email (VARCHAR)
    - edad (INTEGER)
    """

    def __init__(self, db_connection):
        """
        Inicializa con una instancia de PostgreSQLConnection

        Args:
            db_connection (PostgreSQLConnection): Instancia de conexión a la DB
        """
        self.db = db_connection

    def create_user(self, nombre, email, edad):
        """
        Crea un nuevo usuario en la base de datos

        Args:
            nombre (str): Nombre del usuario
            email (str): Email del usuario
            edad (int): Edad del usuario

        Returns:
            int: ID del usuario creado, None si hay error
        """
        try:
            with self.db.get_connection() as conn:
                # Usar text() para queries SQL raw con SQLAlchemy
                query = text("""
                    INSERT INTO users (nombre, email, edad) 
                    VALUES (:nombre, :email, :edad) 
                    RETURNING id
                """)

                result = conn.execute(query, {
                    'nombre': nombre,
                    'email': email,
                    'edad': edad
                })

                # Commit explícito de la transacción
                conn.commit()

                user_id = result.fetchone()[0]
                logger.info(f"Usuario creado con ID: {user_id}")
                return user_id

        except SQLAlchemyError as e:
            logger.error(f"Error al crear usuario: {str(e)}")
            return None

    def read_user(self, user_id):
        """
        Lee un usuario específico por su ID

        Args:
            user_id (int): ID del usuario a buscar

        Returns:
            dict: Datos del usuario o None si no existe
        """
        try:
            with self.db.get_connection() as conn:
                query = text(
                    "SELECT id, nombre, email, edad FROM users WHERE id = :user_id")
                result = conn.execute(query, {'user_id': user_id})

                row = result.fetchone()
                if row:
                    # Convertir Row a diccionario para fácil manejo
                    user_data = {
                        'id': row[0],
                        'nombre': row[1],
                        'email': row[2],
                        'edad': row[3]
                    }
                    logger.info(f"Usuario encontrado: {user_data}")
                    return user_data
                else:
                    logger.info(f"Usuario con ID {user_id} no encontrado")
                    return None

        except SQLAlchemyError as e:
            logger.error(f"Error al leer usuario: {str(e)}")
            return None

    def read_all_users(self):
        """
        Lee todos los usuarios de la base de datos

        Returns:
            list: Lista de diccionarios con datos de usuarios
        """
        try:
            with self.db.get_connection() as conn:
                query = text(
                    "SELECT id, nombre, email, edad FROM users ORDER BY id")
                result = conn.execute(query)

                users = []
                for row in result:
                    user_data = {
                        'id': row[0],
                        'nombre': row[1],
                        'email': row[2],
                        'edad': row[3]
                    }
                    users.append(user_data)

                logger.info(f"Se encontraron {len(users)} usuarios")
                return users

        except SQLAlchemyError as e:
            logger.error(f"Error al leer usuarios: {str(e)}")
            return []

    def update_user(self, user_id, nombre=None, email=None, edad=None):
        """
        Actualiza un usuario existente

        Args:
            user_id (int): ID del usuario a actualizar
            nombre (str, optional): Nuevo nombre
            email (str, optional): Nuevo email
            edad (int, optional): Nueva edad

        Returns:
            bool: True si se actualizó, False en caso contrario
        """
        try:
            # Construir query dinámicamente basado en los parámetros proporcionados
            updates = []
            params = {'user_id': user_id}

            if nombre is not None:
                updates.append("nombre = :nombre")
                params['nombre'] = nombre

            if email is not None:
                updates.append("email = :email")
                params['email'] = email

            if edad is not None:
                updates.append("edad = :edad")
                params['edad'] = edad

            if not updates:
                logger.warning("No se proporcionaron datos para actualizar")
                return False

            with self.db.get_connection() as conn:
                query = text(f"""
                    UPDATE users 
                    SET {', '.join(updates)} 
                    WHERE id = :user_id
                """)

                result = conn.execute(query, params)
                conn.commit()

                if result.rowcount > 0:
                    logger.info(f"Usuario {user_id} actualizado exitosamente")
                    return True
                else:
                    logger.info(
                        f"Usuario {user_id} no encontrado para actualizar")
                    return False

        except SQLAlchemyError as e:
            logger.error(f"Error al actualizar usuario: {str(e)}")
            return False

    def delete_user(self, user_id):
        """
        Elimina un usuario de la base de datos

        Args:
            user_id (int): ID del usuario a eliminar

        Returns:
            bool: True si se eliminó, False en caso contrario
        """
        try:
            with self.db.get_connection() as conn:
                query = text("DELETE FROM users WHERE id = :user_id")
                result = conn.execute(query, {'user_id': user_id})
                conn.commit()

                if result.rowcount > 0:
                    logger.info(f"Usuario {user_id} eliminado exitosamente")
                    return True
                else:
                    logger.info(
                        f"Usuario {user_id} no encontrado para eliminar")
                    return False

        except SQLAlchemyError as e:
            logger.error(f"Error al eliminar usuario: {str(e)}")
            return False
