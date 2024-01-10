import asyncio
import datetime
import logging

from data_extractor import get_warehouse_tariffs
from db_client import DBClient
from wb_delivery_parsing import WildberriesDelivery

logger = logging.getLogger(__name__)


def generate_date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s:%(lineno)d #%(levelname)-8s "
        "[%(asctime)s] - %(name)s - %(message)s",
    )
    logger.info("Start of the script")
    db_client = DBClient()
    await db_client.create_pool()
    logger.info("Database connected")

    uid = "e8a4dd18-5cb0-563e-a7e8-1c6a459606c7"
    query = "SELECT id,token FROM wb_sellers WHERE uid = $1"
    row = await db_client.pool.fetchrow(query, uid)
    token = row["token"]
    start_date = datetime.date(2023, 11, 30)
    end_date = datetime.date.today()
    if token:
        wb_delivery = WildberriesDelivery(token, uid)
        for single_date in generate_date_range(start_date, end_date):
            result = await get_warehouse_tariffs(wb_delivery, single_date)
            print(single_date)
            await db_client.insert_data("wb_warehouses_tariffs", result)
        await wb_delivery.close()
    else:
        logging.error(f"Токен для uid {uid} не найден.")

    await db_client.close_pool()
    logger.info("Database disconnected")


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
    logger.info("End of the program")
