#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ejemplo de conexión a PostgreSQL usando SQLAlchemy y psycopg2
Implementación de operaciones CRUD sin ORM
"""
import logging
from postgres_sql_connection import PostgreSQLConnection
from user_crud import UserCRUD

# Configuración de logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    """
    Función principal que demuestra el uso de las operaciones CRUD
    """
    # Configuración de conexión (ajusta estos valores según tu configuración)
    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'testdb',
        'username': 'postgres',
        'password': 'admin1234'
    }

    try:
        # Inicializar conexión
        db = PostgreSQLConnection(**db_config)

        # Probar conexión
        if not db.test_connection():
            logger.error("No se pudo establecer conexión con la base de datos")
            return

        # Inicializar CRUD
        user_crud = UserCRUD(db)

        # Demostración de operaciones CRUD
        print("\n=== DEMOSTRACIÓN CRUD ===")

        # CREATE - Crear usuarios
        print("\n1. Creando usuarios...")
        user1_id = user_crud.create_user("Juan Pérez", "juan@email.com", 30)
        user2_id = user_crud.create_user("María García", "maria@email.com", 25)
        user3_id = user_crud.create_user(
            "Carlos López", "carlos@email.com", 35)

        # READ - Leer usuario específico
        print("\n2. Leyendo usuario específico...")
        if user1_id:
            user = user_crud.read_user(user1_id)
            if user:
                print(f"Usuario encontrado: {user}")

        # READ - Leer todos los usuarios
        print("\n3. Leyendo todos los usuarios...")
        all_users = user_crud.read_all_users()
        for user in all_users:
            print(f"  - {user}")

        # UPDATE - Actualizar usuario
        print("\n4. Actualizando usuario...")
        if user2_id:
            success = user_crud.update_user(
                user2_id, nombre="María Fernández", edad=26)
            if success:
                updated_user = user_crud.read_user(user2_id)
                print(f"Usuario actualizado: {updated_user}")

        # DELETE - Eliminar usuario
        print("\n5. Eliminando usuario...")
        if user3_id:
            success = user_crud.delete_user(user3_id)
            if success:
                print(f"Usuario {user3_id} eliminado exitosamente")

        # Mostrar usuarios finales
        print("\n6. Usuarios restantes:")
        final_users = user_crud.read_all_users()
        for user in final_users:
            print(f"  - {user}")

    except Exception as e:
        logger.error(f"Error en la aplicación principal: {str(e)}")


if __name__ == "__main__":
    main()
