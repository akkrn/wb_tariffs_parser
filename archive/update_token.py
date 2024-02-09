import asyncio
import logging
from http import HTTPStatus

import aiohttp

from db_client import DBClient

logger = logging.getLogger(__name__)


class WildberriesToken:
    def __init__(self, token: str, supplier_id: str):
        self.client = aiohttp.ClientSession()
        self._token = token
        self._supplier_id = supplier_id

    async def __request(self, *args, **kwargs):
        if "cookies" not in kwargs:
            kwargs["cookies"] = {}
        kwargs["cookies"]["WBToken"] = self._token
        kwargs["cookies"]["x-supplier-id"] = self._supplier_id

        return await self.client.request(*args, **kwargs)

    async def close(self):
        await self.client.close()

    async def get_token_and_login(self) -> str:
        """
        Получение токена и аутентификация
        """
        headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-type": "application/json",
            "Pragma": "no-cache",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        }
        try:
            url = "https://passport.wildberries.ru/api/v2/auth/grant"
            response = await self.__request("POST", url, headers=headers)
            if response.status != HTTPStatus.OK:
                raise Exception(
                    f"Failed to get token, status: {response.status}"
                )
            data = await response.json()
            token = data.get("token")
            url = "https://passport.wildberries.ru/api/v2/auth/login"
            response = await self.__request(
                "POST", url, headers=headers, json={"token": token}
            )
            if response.status != HTTPStatus.OK:
                print(response.status)
                raise Exception(f"Login failed, status: {response.status}")
            cookies = response.cookies
            cookies_dict = {
                cookie.key: cookie.value for cookie in cookies.values()
            }
            token = cookies_dict.get("WBToken")
            print(f"uid: {self._supplier_id}, token: {token}")
            return token
        except Exception as e:
            logger.error(e)


async def update_token_for_uid(db_client: DBClient, uid: str) -> None:
    query = "SELECT token FROM wb_sellers_tariffs WHERE uid=$1"
    row = await db_client.pool.fetchrow(query, uid)
    token = row["token"]
    try:
        if token:
            service = WildberriesToken(token, uid)
            new_token = await service.get_token_and_login()
            if new_token:
                update_query = (
                    "UPDATE wb_sellers_tariffs SET token = $1 WHERE uid = $2"
                )
                await db_client.pool.execute(update_query, new_token, uid)
                logger.info(f"Token updated for uid {uid}")
            else:
                raise Exception("Failed to get token")
            await service.close()
        else:
            raise Exception("Token not found")
    except Exception as e:
        logger.error(f"Token not found for uid {uid}, {e}")


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(filename)s:%(lineno)d #%(levelname)-s "
        "[%(asctime)s] - %(name)s - %(message)s",
    )

    db_client = DBClient()
    await db_client.create_pool()

    uids = [
        "e8a4dd18-5cb0-563e-a7e8-1c6a459606c7",
        "dd027d1f-7065-43ae-a4b7-7484ef4095f6",
    ]
    tasks = [update_token_for_uid(db_client, uid) for uid in uids]
    await asyncio.gather(*tasks)
    await db_client.close_pool()


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
