import asyncio

import pandas as pd

from db_client import DBClient


async def extract_input_redemption_rates(db_client: DBClient, excel_file: str):
    df = pd.read_excel(excel_file)
    category_dict = {}
    rows = await db_client.pool.fetch(
        "SELECT id, category_name, item_name FROM wb_categories"
    )
    for row in rows:
        category_dict[(row["category_name"], row["item_name"])] = row["id"]
    redemption_rates = []
    for index, row in df.iterrows():
        category_name = row["Категория"]
        item_name = row["Предмет"]
        redemption_rate = row["Процент выкупа"]
        redemption_rate = round(float(redemption_rate) / 100, 4)
        if (len(category_name)) == 0:
            category_name = None
        try:
            category_id = category_dict[(category_name, item_name)]
        except KeyError:
            continue
        redemption_rates.append((category_id, redemption_rate))

    insert_query = "INSERT INTO wb_redemption_rates (category_id, redemption_rate) VALUES ($1, $2);"
    await db_client.pool.executemany(insert_query, redemption_rates)


async def main():
    db_client = DBClient()
    await db_client.create_pool()
    query = """
            CREATE TABLE IF NOT EXISTS wb_redemption_rates (
            id SERIAL PRIMARY KEY,
            category_id INT,
            redemption_rate FLOAT,
            date DATE DEFAULT CURRENT_DATE,
            UNIQUE (category_id, date),
            FOREIGN KEY (category_id) REFERENCES wb_categories (id)
            );
            """
    await db_client.pool.execute(query)
    await extract_input_redemption_rates(
        db_client, "data/redemption_rates.xlsx"
    )
    await db_client.close_pool()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
