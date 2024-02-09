import asyncio

from db_client import DBClient
from main import logger


async def fetch_data(db_client: DBClient) -> None:
    """
    Отфильтровывает дубликаты из таблицы wb_logistics_rates и записывает в таблицу wb_commission_rates
    с попутной заменой category_id на category_name и item_name
    """
    categories = await db_client.pool.fetch(
        "SELECT id, category_name, item_name FROM wb_categories"
    )
    category_map = {
        category["id"]: (category["category_name"], category["item_name"])
        for category in categories
    }

    rates = await db_client.pool.fetch(
        "SELECT * FROM wb_logistics_rates ORDER BY category_id, date"
    )

    cleaned_data = []
    last_seen = {}

    for rate in rates:
        category_id = rate["category_id"]
        date = rate["date"]
        fbo_rate = rate["fbo_rate"]
        fbs_rate = rate["fbs_rate"]
        china_rate = rate["china_rate"]

        key = (fbs_rate, fbo_rate, china_rate)

        if category_id not in last_seen:
            last_seen[category_id] = key
            category_name, item_name = category_map.get(category_id)
            cleaned_data.append(
                {
                    "category_name": category_name,
                    "item_name": item_name,
                    "date": date,
                    "fbo_rate": fbo_rate,
                    "fbs_rate": fbs_rate,
                    "china_rate": china_rate,
                }
            )
        else:
            if last_seen[category_id] != key:
                last_seen[category_id] = key
                category_name, item_name = category_map.get(category_id)
                cleaned_data.append(
                    {
                        "category_name": category_name,
                        "item_name": item_name,
                        "date": date,
                        "fbo_rate": fbo_rate,
                        "fbs_rate": fbs_rate,
                        "china_rate": china_rate,
                    }
                )

    await db_client.insert_data("wb_commission_rates", cleaned_data)


async def main():
    db_client = DBClient()
    await db_client.create_pool()
    await db_client.create_tables()
    logger.info("Database connected")

    await fetch_data(db_client)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
