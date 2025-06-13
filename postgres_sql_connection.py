#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ejemplo de conexión a PostgreSQL usando SQLAlchemy y psycopg2
Implementación de operaciones CRUD sin ORM
"""

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PostgreSQLConnection:
    """
    Clase para manejar la conexión a PostgreSQL usando SQLAlchemy
    """

    def __init__(self, host='localhost', port=5432, database='testdb',
                 username='postgres', password='admin1234'):
        """
        Inicializa los parámetros de conexión

        Args:
            host (str): Dirección del servidor PostgreSQL
            port (int): Puerto de conexión (por defecto 5432)
            database (str): Nombre de la base de datos
            username (str): Usuario de la base de datos
            password (str): Contraseña del usuario
        """
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password

        # Construcción de la URL de conexión para SQLAlchemy
        # Formato: postgresql+psycopg2://usuario:password@host:puerto/database
        self.connection_url = (
            f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
        )

        # Inicialización del engine de SQLAlchemy
        self.engine = None
        self._create_engine()

    def _create_engine(self):
        """
        Crea el engine de SQLAlchemy con configuraciones específicas
        """
        try:
            # Parámetros del pool de conexiones
            self.engine = create_engine(
                self.connection_url,
                pool_size=10,              # Número de conexiones permanentes en el pool
                max_overflow=20,           # Conexiones adicionales permitidas
                pool_timeout=30,           # Tiempo máximo de espera por una conexión
                # Tiempo antes de reciclar una conexión (segundos)
                pool_recycle=3600,
                # True para mostrar SQL generado (debug)
                echo=False
            )
            logger.info("Engine de SQLAlchemy creado exitosamente")
        except Exception as e:
            logger.error(f"Error al crear engine: {str(e)}")
            raise

    # En Python, un contextmanager es un objeto o función que se utiliza junto con la 
    # declaración with para gestionar recursos de forma automatizada. 
    # Permite definir acciones que se ejecutan al entrar (inicialización) y al salir (limpieza) de un bloque de 
    # código, independientemente de si ocurre una excepción o no. 
    @contextmanager
    def get_connection(self):
        """
        Context manager para obtener y manejar conexiones de forma segura

        Yields:
            connection: Objeto de conexión de SQLAlchemy
        """
        connection = None
        try:
            connection = self.engine.connect()
            logger.info("Conexión establecida exitosamente")
            yield connection
        except SQLAlchemyError as e:
            logger.error(f"Error de SQLAlchemy: {str(e)}")
            if connection:
                connection.rollback()
            raise
        except Exception as e:
            logger.error(f"Error general: {str(e)}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
                logger.info("Conexión cerrada")

    def test_connection(self):
        """
        Prueba la conexión a la base de datos

        Returns:
            bool: True si la conexión es exitosa, False en caso contrario
        """
        try:
            with self.get_connection() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"Conexión exitosa. Versión PostgreSQL: {version}")
                return True
        except Exception as e:
            logger.error(f"Error en prueba de conexión: {str(e)}")
            return False
