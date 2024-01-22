import asyncio
import datetime
import logging

from data_extractor import (
    get_weekly_rating,
    get_logistics_rates,
    get_warehouse_tariffs,
    get_return_tariffs,
    get_acceptance_coefficients,
)
from db_client import DBClient
from wb_delivery_parsing import WildberriesDelivery

logger = logging.getLogger(__name__)


UIDS = [
    "e8a4dd18-5cb0-563e-a7e8-1c6a459606c7",
    "dd027d1f-7065-43ae-a4b7-7484ef4095f6",
]


async def get_token(db_client: DBClient, uid: str) -> list[dict]:
    """
    Получение токенов
    """
    # TODO add caching for user data
    query = "SELECT id, token, uid FROM wb_sellers WHERE uid = $1"
    row = await db_client.pool.fetchrow(query, uid)
    return row


async def process_many_clients(db_client: DBClient, uid: str) -> list:
    """
    Запуск и выполнение асинхронных задач, результат которых будет
    отличаться для каждого клиента
    """
    row = await get_token(db_client, uid)
    token = row.get("token")
    if token:
        wb_delivery = WildberriesDelivery(token, uid)
        tasks = [
            get_weekly_rating(row.get("id"), wb_delivery),
        ]
        result = await asyncio.gather(*tasks)
        await wb_delivery.close()
        return result


async def process_one_client(db_client: DBClient, uid: str) -> list:
    """
    Запуск и выполнение асинхронных задач, результат которых не зависит от uid
    """
    row = await get_token(db_client, uid)
    token = row.get("token")
    if token:
        wb_delivery = WildberriesDelivery(token, uid)
        tasks = [
            get_warehouse_tariffs(
                wb_delivery,
                db_client,
                datetime.date.today() + datetime.timedelta(days=3),
            ),
            # get_logistics_rates(wb_delivery, db_client),
            get_acceptance_coefficients(
                wb_delivery,
                db_client,
                datetime.date.today() + datetime.timedelta(days=3),
            ),
            get_return_tariffs(
                wb_delivery, datetime.date.today() + datetime.timedelta(days=3)
            ),
        ]
        result = await asyncio.gather(*tasks)
        await wb_delivery.close()
        return result
    else:
        logging.error(f"Токен для uid {uid} не найден.")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s:%(lineno)d #%(levelname)-8s "
        "[%(asctime)s] - %(name)s - %(message)s",
    )
    logger.info("Start of the program")

    db_client = DBClient()
    await db_client.create_pool()
    await db_client.create_tables()
    logger.info("Database connected")

    tasks = [process_many_clients(db_client, uid) for uid in UIDS]
    result = await asyncio.gather(*tasks)
    tables = ["wb_seller_logistics_coefficients"]
    for row in result:
        for i in range(len(row)):
            if row[i]:
                await db_client.insert_data(tables[i], row[i])
                logger.debug(f"Data inserted into {tables[i]}")

    # Запуск асинхронных задач, не зависящих от UID
    tasks = process_one_client(db_client, UIDS[0])
    result = await asyncio.gather(tasks)
    tables = [
        "wb_warehouses_tariffs",
        # "wb_logistics_rates",
        "wb_acceptance_coefficients",
        "wb_return_tariffs",
    ]
    for row in result:
        print(row)
        for i in range(len(row)):
            if row[i]:
                await db_client.insert_data(tables[i], row[i])
                logger.debug(f"Data inserted into {tables[i]}")

    await db_client.close_pool()
    logger.info("Database disconnected")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    logger.info("End of the program")
