import asyncio

import pandas as pd

from db_client import DBClient


async def main():
    db_client = DBClient()
    await db_client.create_pool()
    df = pd.read_excel("data/categories.xlsx")
    values = []
    for index, row in df.iterrows():
        if row["Категория"] != row["Категория"]:
            row["Категория"] = None
        print(row["Категория"])
        category_name = row["Категория"]
        item_name = row["Предмет"]
        values.append((category_name, item_name))
    insert_query = (
        "INSERT INTO wb_categories (category_name, item_name) VALUES ($1, $2);"
    )
    await db_client.pool.executemany(insert_query, values)
    await db_client.close_pool()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
