import asyncio
import datetime
import logging
import sentry_sdk
from asyncpg import Record

from data_extractor import WbDataExtractor

from db_client import DBClient
from exceptions import FailedGetDataException, AuthException
from wb_parser import WbParser

logger = logging.getLogger(__name__)


async def execute_tasks(db_client: DBClient, seller: Record, task_creator):
    """
    Общая функция для инициализации и выполнения задач.
    """
    refresh_token = seller.get("refresh_token")
    device_id = seller.get("device_id")
    supplier_id = seller.get("supplier_id")
    name = seller.get("name")

    if not (refresh_token and device_id):
        logging.error(f"Токен для селлера: {name} не найден.")
        return

    wb_parser = WbParser(refresh_token, supplier_id, device_id)
    wb_data_extractor = WbDataExtractor(db_client, wb_parser)
    try:
        tasks = await task_creator(wb_data_extractor)
        return await asyncio.gather(*tasks)
    except AuthException:
        logging.warning(f"Некорректная авторизация поставщика: {name}")
    except FailedGetDataException as e:
        logging.warning(e)
    except Exception as e:
        sentry_sdk.capture_exception(e)
    finally:
        await wb_parser.close()



async def get_individual_data(db_client: DBClient, seller: Record) -> None:
    async def task_creator(wb_data_extractor):
        return [
            wb_data_extractor.insert_weekly_rating(seller.get("id")),
        ]
    await execute_tasks(db_client, seller, task_creator)


async def get_common_data(db_client: DBClient, seller: Record) -> None:
    async def task_creator(wb_data_extractor):
        today = datetime.date.today()
        return [
            *[wb_data_extractor.insert_warehouse_tariffs(today + datetime.timedelta(days=delta_days)) for delta_days in
              range(3)],
            wb_data_extractor.insert_commission_rates(),
            wb_data_extractor.insert_acceptance_coefficients(today),
            wb_data_extractor.insert_return_tariffs(today),
        ]
    await execute_tasks(db_client, seller, task_creator)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s:%(lineno)d #%(levelname)-8s "
               "[%(asctime)s] - %(name)s - %(message)s",
    )
    logger.info("Start of the program")

    db_client = DBClient()
    await db_client.create_pool()
    logger.info("Database connected")

    query = "SELECT * FROM wb_sellers_tariffs"
    sellers = await db_client.pool.fetch(query)

    tasks = [get_individual_data(db_client, seller) for seller in sellers]
    await asyncio.gather(*tasks, get_common_data(db_client, sellers[0]))


    await db_client.close_pool()
    logger.info("Database disconnected")


if __name__ == "__main__":
    sentry_sdk.init(
        "https://8f38ea66aa11448e8db646f2e8258781@gt.botkompot.ru/7"
    )
    asyncio.run(main())
    logger.info("End of the program")
