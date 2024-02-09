import asyncio

import pandas as pd

from db_client import DBClient
from utils import str_to_float


async def extract_input_fulfillments_rates(
    db_client: DBClient, excel_file: str
):
    df = pd.read_excel(excel_file)
    warehouses_dict = {}
    rows = await db_client.pool.fetch("SELECT id, name FROM wb_warehouses")
    for row in rows:
        warehouses_dict[row["name"]] = row["id"]
    fulfillments_rates = []
    for index, row in df.iterrows():
        fulfillment_name = row["Фулфилмент"]
        try:
            warehouse_id = warehouses_dict[row["Склад"]]
        except KeyError:
            print(f"Склад {row['Склад']} не найден в базе данных")
            continue
        acceptance_rate = str_to_float(row["Приемка"])
        storage_rate = str_to_float(row["Хранение"])
        if row["Доставка короба"] == "нет":
            box_delivery_rate = None
        else:
            box_delivery_rate = str_to_float(row["Доставка короба"])
        if row["Доставка паллета"] == "нет":
            pallet_delivery_rate = None
        else:
            pallet_delivery_rate = str_to_float(row["Доставка паллета"])

        fulfillments_rates.append(
            (
                fulfillment_name,
                warehouse_id,
                acceptance_rate,
                storage_rate,
                box_delivery_rate,
                pallet_delivery_rate,
            )
        )

    insert_query = "INSERT INTO wb_fulfillments (fulfillment_name, warehouse_id, acceptance_rate, storage_rate, box_delivery_rate, pallet_delivery_rate) VALUES ($1, $2, $3, $4, $5, $6);"
    await db_client.pool.executemany(insert_query, fulfillments_rates)


async def main():
    db_client = DBClient()
    await db_client.create_pool()
    query = """
            CREATE TABLE IF NOT EXISTS wb_fulfillments (
            id SERIAL PRIMARY KEY,
            fulfillment_name VARCHAR(50),
            warehouse_id INT,
            acceptance_rate FLOAT,
            storage_rate FLOAT,
            box_delivery_rate FLOAT,
            pallet_delivery_rate FLOAT,
            date DATE DEFAULT CURRENT_DATE,
            UNIQUE (fulfillment_name, warehouse_id, date),
            FOREIGN KEY (warehouse_id) REFERENCES wb_warehouses (id)
            );
            """
    await db_client.pool.execute(query)
    await extract_input_fulfillments_rates(
        db_client, "data/fulfillments.xlsx"
    )
    await db_client.close_pool()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
