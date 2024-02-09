import datetime
import json
from http import HTTPStatus

from aiohttp_retry import ExponentialRetry, RetryClient

from exceptions import AuthException, FailedGetDataException


class WbParser:
    def __init__(
            self,
            refresh_token: str,
            supplier_id: str,
            device_id: str,
    ):
        retry_options = ExponentialRetry(attempts=5, statuses={429, })
        self._client = RetryClient(raise_for_status=False, retry_options=retry_options)
        self._refresh_token = refresh_token
        self._supplier_id = supplier_id
        self._device_id = device_id
        self._validation_key_cache = {}

    HEADERS = {
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }
    AUTH_URL = "https://seller-auth.wildberries.ru/auth/v2/auth/slide-v3"

    @staticmethod
    async def __handle_response(response) -> dict:
        if response.status == 401:
            raise AuthException("Invalid token")
        if response.status != HTTPStatus.OK:
            error_message = await response.json()
            raise FailedGetDataException(f"Failed to get data, status: {response.status}\nMessage: {error_message}")
        return await response.json()

    async def __request(self, method: str, url: str, payload: dict = None, cookies: dict = None) -> dict:
        """
        Метод для получения validation_key и токена для последующих запросов
        """
        date = datetime.date.today()
        if date in self._validation_key_cache:
            updated_cookies = self._validation_key_cache[date]
        else:
            initial_cookies = {
                "wbx-refresh": self._refresh_token,
                "wbx-seller-device-id": self._device_id,
            }
            response = await self._client.request("POST", self.AUTH_URL, headers=self.HEADERS, cookies=initial_cookies)
            response_data = await self.__handle_response(response)
            validation_key = response.cookies.get("wbx-validation-key").value
            token = response_data["payload"]["access_token"]
            updated_cookies = {
                "wbx-validation-key": validation_key,
                "WBTokenV3": token,
                "x-supplier-id-external": self._supplier_id,
                "x-supplier-id": self._supplier_id

            }
            self._validation_key_cache[date] = updated_cookies
        updated_cookies.update(cookies) if cookies else None
        response = await self._client.request(method, url, headers=self.HEADERS, cookies=updated_cookies,
                                              data=json.dumps(payload) if payload else None)
        return await self.__handle_response(response)

    async def parse_weekly_rating(self) -> dict:
        """
        Парсинг коэфициента логистики и индекса локализации
        """
        url = (
            "https://seller.wildberries.ru/ns/categories-info/suppliers"
            "-portal-analytics/api/v1/weekly-rating"
        )

        response_data = await self.__request("GET", url)
        return response_data

    async def parse_warehouses_tariffs(self, date=datetime.date.today()) -> dict:
        """
        Парсинг тарифов по ящикам и паллетам на складах
        """
        url = f"https://seller-weekly-report.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/tariffs-period?date={date}&short=false"
        payload = {"box": "asc"}
        response_data = await self.__request("POST", url, payload=payload)
        return response_data

    async def parse_commission_rates(self) -> dict:
        """
        Парсинг коммисий по категориям
        """
        url = "https://seller.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/categories"
        payload = {"sort": "name", "order": "asc"}
        cookies = {"external-locale": "ru", "locale": "ru",}
        response_data = await self.__request("POST", url, payload=payload, cookies=cookies)
        return response_data

    async def parse_acceptance_coefficients(self, date=datetime.date.today()) -> dict:
        """
        Парсинг коммисий приемки, данные выгружаются на неделю вперед
        По умолчанию идет отсчет от "сегодня" и на 7 дней вперед
        """
        url = "https://seller-supply.wildberries.ru/ns/sm-supply/supply-manager/api/v1/supply/acceptanceCoefficientsReport"
        payload = {
                 "params": {
                     "dateTo": f"{date+datetime.timedelta(7)}T23:59:00.000Z",
                     "dateFrom": f"{date}T00:00:00.000Z",
                 },
                 "jsonrpc": "2.0",
                 "id": "json-rpc_10",
        }
        response_data = await self.__request("POST", url, payload=payload)
        return response_data

    async def return_tariffs(self, date=datetime.date.today()) -> dict:
        """
        Парсинг ставок за логистику по возвратам
        По умолчанию данные выгружаются за "сегодня", доступны на неделю вперед
        """
        url = f"https://seller-weekly-report.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/return-tariffs?date={date}"
        response_data = await self.__request("GET", url)
        return response_data


    async def parse_categories_data(self) -> dict:
        """
        Парсинг всех категорий и подкатегорий
        """
        url = "https://seller.wildberries.ru/ns/categories-info/suppliers-portal-analytics/api/v1/subjects"
        cookies = {"external-locale": "ru", "locale": "ru",}
        response_data = await self.__request("GET", url, cookies=cookies)
        return response_data

    async def close(self):
        await self._client.close()