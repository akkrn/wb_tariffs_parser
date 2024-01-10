import datetime
import json
import logging
from http import HTTPStatus

import aiohttp

logger = logging.getLogger(__name__)


class WildberriesDelivery:
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

    async def __get_token_and_login(self, base_url: str) -> dict:
        """
        Получение токена и аутентификация в поддомене
        """
        headers = {
            "Accept": "*/*",
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
            url = (
                f"https://{base_url}.wildberries.ru/passport/api/v2/auth/login"
            )
            response = await self.__request(
                "POST", url, headers=headers, json={"token": token}
            )
            if response.status != HTTPStatus.OK:
                raise Exception(f"Login failed, status: {response.status}")
            cookies = response.cookies
            cookies_dict = {
                cookie.key: cookie.value for cookie in cookies.values()
            }
            subdomain_token = cookies_dict.get("WBToken")
            return subdomain_token
        except Exception as e:
            logger.error(e)

    async def close(self):
        await self.client.close()

    async def weekly_rating(self) -> dict:
        """
        Парсинг коэфициента логистики и индекса локализации
        """
        try:
            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                "Connection": "keep-alive",
                "Content-type": "application/json",
                "Referer": "https://seller.wildberries.ru/dynamic-product-categories/delivery",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }
            url = (
                "https://seller.wildberries.ru/ns/categories-info/suppliers"
                "-portal-analytics/api/v1/weekly-rating"
            )

            base_url = url.split("https://")[1].split(".wildberries.ru")[0]
            subdomain_token = await self.__get_token_and_login(base_url)

            cookies = {
                "WBToken": subdomain_token,
                "x-supplier-id": self._supplier_id,
            }
            response = await self.client.request(
                "GET", url, headers=headers, cookies=cookies
            )
            if response.status != HTTPStatus.OK:
                raise Exception(
                    f"Failed to get data, status: {response.status}"
                )
            return await response.json()

        except Exception as e:
            logger.error(e)

    async def warehouses_tariffs(self, date=datetime.date.today()) -> dict:
        """
        Парсинг тарифов по ящикам и паллетам на складах
        """
        try:
            url = f"https://seller-weekly-report.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/tariffs-period?date={date}&short=false"

            base_url = url.split("https://")[1].split(".wildberries.ru")[0]
            subdomain_token = await self.__get_token_and_login(base_url)

            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                "Connection": "keep-alive",
                "Content-type": "application/json",
                "Origin": "https://seller.wildberries.ru",
                "Referer": "https://seller.wildberries.ru/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }
            cookies = {
                "WBToken": subdomain_token,
                "x-supplier-id-external": self._supplier_id,
            }
            payload = json.dumps({"box": "asc"})
            response = await self.client.request(
                "POST", url, headers=headers, data=payload, cookies=cookies
            )
            if response.status != HTTPStatus.OK:
                raise Exception(
                    f"Failed to get data, status: {response.status}"
                )
            return await response.json()
        except Exception as e:
            logger.error(e)

    async def logistics_rates(self) -> dict:
        """
        Парсинг коммисий по категориям
        """
        try:
            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                "Connection": "keep-alive",
                "Content-type": "application/json",
                "Origin": "https://seller.wildberries.ru",
                "Referer": "https://seller.wildberries.ru/dynamic-product-categories/commission",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }
            payload = json.dumps({"sort": "name", "order": "asc"})
            url = "https://seller.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/categories"

            base_url = url.split("https://")[1].split(".wildberries.ru")[0]
            subdomain_token = await self.__get_token_and_login(base_url)
            cookies = {
                "WBToken": subdomain_token,
                "x-supplier-id": self._supplier_id,
                "external-locale": "ru",
            }
            response = await self.client.request(
                "POST", url, headers=headers, data=payload, cookies=cookies
            )
            if response.status != HTTPStatus.OK:
                raise Exception(
                    f"Failed to get data, status: {response.status}"
                )
            return await response.json()
        except Exception as e:
            logger.error(e)

    async def acceptance_сoefficients(
        self, date=datetime.date.today()
    ) -> dict:
        """
        Парсинг коммисий приемки, данные выгружаются на неделю вперед
        По умолчанию идет отсчет от "сегодня" и на 7 дней вперед
        """
        try:
            headers = {
                "Accept": "*/*",
                "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
                "Connection": "keep-alive",
                "Content-type": "application/json",
                "Origin": "https://seller.wildberries.ru",
                "Referer": "https://seller.wildberries.ru/",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }
            payload = json.dumps(
                {
                    "params": {
                        "dateTo": f"{date+datetime.timedelta(7)}T23:59:00.000Z",
                        "dateFrom": f"{date}T00:00:00.000Z",
                    },
                    "jsonrpc": "2.0",
                    "id": "json-rpc_10",
                }
            )
            url = "https://seller-supply.wildberries.ru/ns/sm-supply/supply-manager/api/v1/supply/acceptanceCoefficientsReport"
            base_url = url.split("https://")[1].split(".wildberries.ru")[0]
            subdomain_token = await self.__get_token_and_login(base_url)
            cookies = {
                "WBToken": subdomain_token,
                "x-supplier-id-external": self._supplier_id,
            }
            response = await self.client.request(
                "POST", url, headers=headers, data=payload, cookies=cookies
            )
            if response.status != HTTPStatus.OK:
                raise Exception(
                    f"Failed to get data, status: {response.status}"
                )
            return await response.json()
        except Exception as e:
            logger.error(e)

    async def return_tariffs(self, date=datetime.date.today()) -> dict:
        """
        Парсинг ставок за логистику по возвратам
        По умолчанию данные выгружаются за "сегодня", доступны на неделю вперед
        """
        try:
            url = f"https://seller-weekly-report.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/return-tariffs?date={date}"
            base_url = url.split("https://")[1].split(".wildberries.ru")[0]
            subdomain_token = await self.__get_token_and_login(base_url)

            headers = {
                "authority": "seller-weekly-report.wildberries.ru",
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9,ru;q=0.8",
                "content-type": "application/json",
                "origin": "https://seller.wildberries.ru",
                "referer": "https://seller.wildberries.ru/",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            }
            cookies = {
                "WBToken": subdomain_token,
                "x-supplier-id-external": self._supplier_id,
            }
            response = await self.client.request(
                "GET", url, headers=headers, cookies=cookies
            )
            if response.status != HTTPStatus.OK:
                raise Exception(
                    f"Failed to get data, status: {response.status}"
                )
            return await response.json()
        except Exception as e:
            logger.error(e)
