
import asyncpg
import os

from dotenv import load_dotenv

load_dotenv()


class DBClient:
    def __init__(self):
        self.db_host = os.getenv("DB_HOST")
        self.db_port = int(os.getenv("DB_PORT"))
        self.db_user = os.getenv("POSTGRES_USER")
        self.db_password = os.getenv("POSTGRES_PASSWORD")
        self.db_name = os.getenv("POSTGRES_DB")
        self.pool = None

    async def create_pool(self):
        self.pool = await asyncpg.create_pool(
            user=self.db_user,
            password=self.db_password,
            database=self.db_name,
            host=self.db_host,
            port=self.db_port,
        )

    async def close_pool(self):
        await self.pool.close()

    async def create_tables(self):
        queries = [
            """
            CREATE TABLE IF NOT EXISTS wb_sellers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                uid VARCHAR(50) UNIQUE,
                token VARCHAR(150) UNIQUE   
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS wb_categories (
                id SERIAL PRIMARY KEY,
                category_name VARCHAR,
                item_name VARCHAR,
                UNIQUE (category_name, item_name)
            );
            """,
            """ 
            CREATE TABLE IF NOT EXISTS wb_warehouses (
                id SERIAL PRIMARY KEY,
                name VARCHAR UNIQUE
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS wb_seller_logistics_coefficients (
                id SERIAL PRIMARY KEY,
                seller_id INTEGER,
                logistics_coefficient FLOAT,
                localization_index FLOAT,
                date DATE,
                UNIQUE (seller_id, date),
                FOREIGN KEY (seller_id) REFERENCES wb_sellers (id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS wb_logistics_rates (
                id SERIAL PRIMARY KEY,
                category_id INTEGER,
                fbo_rate FLOAT,
                fbs_rate FLOAT,
                china_rate FLOAT,
                date DATE,
                UNIQUE (category_id, date),
                FOREIGN KEY (category_id) REFERENCES wb_categories (id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS wb_warehouses_tariffs (
                id SERIAL PRIMARY KEY,
                date DATE,
                warehouse_name VARCHAR,
                box_delivery_and_storage_expr FLOAT,
                box_delivery_base FLOAT,
                box_delivery_liter FLOAT,
                box_storage_base FLOAT,
                box_storage_liter FLOAT,
                pallet_delivery_expr FLOAT,
                pallet_delivery_value_base FLOAT,
                pallet_delivery_value_liter FLOAT,
                pallet_storage_expr FLOAT,
                pallet_storage_value_expr FLOAT,
                warehouse_id INT,
                box_delivery_and_storage_color_expr VARCHAR, 
                box_delivery_and_storage_color_expr_next VARCHAR, 
                box_delivery_and_storage_diff_sign INTEGER, 
                box_delivery_and_storage_diff_sign_next INTEGER, 
                box_delivery_and_storage_expr_next FLOAT, 
                box_delivery_and_storage_visible_expr FLOAT, 
                pallet_delivery_color_expr VARCHAR, 
                pallet_delivery_color_expr_next VARCHAR, 
                pallet_delivery_diff_sign INTEGER, 
                pallet_delivery_diff_sign_next INTEGER, 
                pallet_delivery_expr_next FLOAT, 
                pallet_storage_color_expr VARCHAR, 
                pallet_storage_color_expr_next VARCHAR, 
                pallet_storage_diff_sign INTEGER, 
                pallet_storage_diff_sign_next INTEGER, 
                pallet_storage_expr_next FLOAT, 
                pallet_visible_expr FLOAT,     
                UNIQUE (date, warehouse_name),
                FOREIGN KEY (warehouse_id) REFERENCES wb_warehouses (id)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS wb_acceptance_coefficients (
                id SERIAL PRIMARY KEY,
                date DATE,
                acceptance_type INT,
                coefficient INT,
                warehouse_id_from_json INT,
                warehouse_name VARCHAR(50),
                warehouse_id INT,
                FOREIGN KEY (warehouse_id) REFERENCES wb_warehouses (id),
                UNIQUE(date,warehouse_id,acceptance_type)
                );
            """,
            """
            CREATE TABLE IF NOT EXISTS wb_return_tariffs (
                id SERIAL PRIMARY KEY,
                date DATE,
                warehouse_sort INT,
                warehouse_name VARCHAR(255) NOT NULL,
                delivery_dump_sup_office_expr VARCHAR(255),
                delivery_dump_sup_office_base FLOAT,
                delivery_dump_sup_office_liter FLOAT,
                delivery_dump_sup_courier_expr VARCHAR(255),
                delivery_dump_sup_courier_base FLOAT,
                delivery_dump_sup_courier_liter FLOAT,
                delivery_dump_sup_return_expr VARCHAR(255),
                delivery_dump_kgt_office_expr VARCHAR(255),
                delivery_dump_kgt_office_base FLOAT,
                delivery_dump_kgt_office_liter FLOAT,
                delivery_dump_kgt_return_expr VARCHAR(255),
                delivery_dump_srg_office_expr VARCHAR(255),
                delivery_dump_srg_return_expr VARCHAR(255),
                UNIQUE (warehouse_name, date)
                );
            """,
        ]
        for query in queries:
            await self.pool.execute(query)

    async def insert_data(self, table_name, data):
        if isinstance(data, dict):
            data = [data]
        keys = data[0].keys()
        columns = ", ".join(keys)
        values_placeholders = ", ".join(f"${i + 1}" for i in range(len(keys)))
        query = (
            f"INSERT INTO {table_name} ({columns}) VALUES ("
            f"{values_placeholders}) ON CONFLICT DO NOTHING;"
        )
        print("DEBUG QUERY:", query)
        values = [tuple(item[key] for key in keys) for item in data]
        await self.pool.executemany(query, values)

    async def clear_database(self):  # Тестовый метод для очистки базы данных
        tables = ["wb_sellers"]
        for table in tables:
            query = f"DELETE FROM {table}"
            await self.pool.execute(query)
