import datetime
import logging

from db_client import DBClient
from utils import str_to_float
from wb_parser import WbParser

logger = logging.getLogger(__name__)


class WbDataExtractor:
    def __init__(self, db_client: DBClient, wb_parser: WbParser):
        self._db_client = db_client
        self._wb_parser = wb_parser

    async def get_weekly_rating(self, row_id: int) -> dict:
        """
        Извлечение данных о коэфициенте логистики и индексе локализации
        Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
        """
        weekly_rating_data = await self._wb_parser.parse_weekly_rating()
        weekly_rating = {
            "seller_id": row_id,
            "logistics_coefficient": weekly_rating_data["data"][
                "logisticAndStorage"
            ]["rating"],
            "localization_index": weekly_rating_data["data"]["localization"][
                "index"
            ],
            "date": datetime.date.today(),
        }
        return weekly_rating

    async def insert_weekly_rating(self, seller_id) -> None:
        """
        Вставка данных об индексе локализации и коэффициенте логистики
        """
        weekly_rating = await self.get_weekly_rating(seller_id)
        if weekly_rating:
            await self._db_client.insert_data(
                "wb_seller_logistics_coefficients", weekly_rating
            )
        else:
            logger.info(f"Данные для селлера с id: {seller_id} не получены.")

    async def get_warehouse_tariffs(
        self,
        date=datetime.date.today(),
    ) -> list[dict]:
        """
        Извлечение данных о тарифах скаладов
        Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
        """
        warehouse_tariffs_data = await self._wb_parser.parse_warehouses_tariffs(date)
        warehouse_tariffs = []
        warehouses_dict = {}
        rows = await self._db_client.pool.fetch(
            "SELECT id, name FROM wb_warehouses"
        )
        for row in rows:
            warehouses_dict[row["name"]] = row["id"]

        for warehouse in warehouse_tariffs_data["data"]["warehouseList"]:
            warehouse_name = warehouse.get("warehouseName")
            warehouse_id = warehouses_dict.get(warehouse_name)
            if warehouse_id is None:
                continue
            warehouse_data = {
                "warehouse_name": warehouse_name,
                "date": date,
                "box_delivery_and_storage_expr": str_to_float(
                    warehouse.get("boxDeliveryAndStorageExpr")
                ),
                "box_delivery_base": str_to_float(
                    warehouse.get("boxDeliveryBase")
                ),
                "box_delivery_liter": str_to_float(
                    warehouse.get("boxDeliveryLiter")
                ),
                "box_storage_base": str_to_float(
                    warehouse.get("boxStorageBase")
                ),
                "box_storage_liter": str_to_float(
                    warehouse.get("boxStorageLiter")
                ),
                "pallet_delivery_expr": str_to_float(
                    warehouse.get("palletDeliveryExpr")
                ),
                "pallet_delivery_value_base": str_to_float(
                    warehouse.get("palletDeliveryValueBase")
                ),
                "pallet_delivery_value_liter": str_to_float(
                    warehouse.get("palletDeliveryValueLiter")
                ),
                "pallet_storage_expr": str_to_float(
                    warehouse.get("palletStorageExpr")
                ),
                "pallet_storage_value_expr": str_to_float(
                    warehouse.get("palletStorageValueExpr")
                ),
                "warehouse_id": int(warehouse_id),
                "box_delivery_and_storage_color_expr": warehouse.get(
                    "boxDeliveryAndStorageColorExpr"
                ),
                "box_delivery_and_storage_color_expr_next": warehouse.get(
                    "boxDeliveryAndStorageColorExprNext"
                ),
                "box_delivery_and_storage_diff_sign": warehouse.get(
                    "boxDeliveryAndStorageDiffSign"
                ),
                "box_delivery_and_storage_diff_sign_next": warehouse.get(
                    "boxDeliveryAndStorageDiffSignNext"
                ),
                "box_delivery_and_storage_expr_next": str_to_float(
                    warehouse.get("boxDeliveryAndStorageExprNext")
                ),
                "box_delivery_and_storage_visible_expr": str_to_float(
                    warehouse.get("boxDeliveryAndStorageVisibleExpr")
                ),
                "pallet_delivery_color_expr": warehouse.get(
                    "palletDeliveryColorExpr"
                ),
                "pallet_delivery_color_expr_next": warehouse.get(
                    "palletDeliveryColorExprNext"
                ),
                "pallet_delivery_diff_sign": warehouse.get(
                    "palletDeliveryDiffSign"
                ),
                "pallet_delivery_diff_sign_next": warehouse.get(
                    "palletDeliveryDiffSignNext"
                ),
                "pallet_delivery_expr_next": str_to_float(
                    warehouse.get("palletDeliveryExprNext")
                ),
                "pallet_storage_color_expr": warehouse.get(
                    "palletStorageColorExpr"
                ),
                "pallet_storage_color_expr_next": warehouse.get(
                    "palletStorageColorExprNext"
                ),
                "pallet_storage_diff_sign": warehouse.get(
                    "palletStorageDiffSign"
                ),
                "pallet_storage_diff_sign_next": warehouse.get(
                    "palletStorageDiffSignNext"
                ),
                "pallet_storage_expr_next": str_to_float(
                    warehouse.get("palletStorageExprNext")
                ),
                "pallet_visible_expr": str_to_float(
                    warehouse.get("palletVisibleExpr")
                ),
            }
            warehouse_tariffs.append(warehouse_data)
        return warehouse_tariffs

    async def insert_warehouse_tariffs(
        self, date=datetime.date.today()
    ) -> None:
        """
        Вставка данных о тарифах складов
        """
        warehouse_tariffs = await self.get_warehouse_tariffs(date)
        if warehouse_tariffs:
            await self._db_client.insert_update_data(
                "wb_warehouses_tariffs",
                warehouse_tariffs,
                conflict_target="wb_warehouses_tariffs_date_warehouse_name_key",
                update_fields=[
                    "box_delivery_and_storage_expr",
                    "box_delivery_base",
                    "box_delivery_liter",
                    "box_storage_base",
                    "box_storage_liter",
                    "pallet_delivery_expr",
                    "pallet_delivery_value_base",
                    "pallet_delivery_value_liter",
                    "pallet_storage_expr",
                    "pallet_storage_value_expr",
                    "box_delivery_and_storage_color_expr",
                    "box_delivery_and_storage_color_expr_next",
                    "box_delivery_and_storage_diff_sign",
                    "box_delivery_and_storage_diff_sign_next",
                    "box_delivery_and_storage_expr_next",
                    "box_delivery_and_storage_visible_expr",
                    "pallet_delivery_color_expr",
                    "pallet_delivery_color_expr_next",
                    "pallet_delivery_diff_sign",
                    "pallet_delivery_diff_sign_next",
                    "pallet_delivery_expr_next",
                    "pallet_storage_color_expr",
                    "pallet_storage_color_expr_next",
                    "pallet_storage_diff_sign",
                    "pallet_storage_diff_sign_next",
                    "pallet_storage_expr_next",
                    "pallet_visible_expr",
                ],
            )
        else:
            logger.info("Тарифов складов на эту дату нет")

    async def get_commission_rates(
        self,
    ) -> list[dict]:
        """
        Извлечение данных о ставках логистики по категориям товаров
        Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
        """
        query = """ WITH ranked_data AS (SELECT *,
            ROW_NUMBER() OVER (PARTITION BY category_name, item_name ORDER BY date DESC) as rn
            FROM wb_commission_rates) SELECT category_name, item_name, date, fbo_rate, fbs_rate, china_rate
            FROM ranked_data WHERE rn = 1;
            """
        last_commission_rates = await self._db_client.pool.fetch(query)
        last_commission_dict = {
            (row[0], row[1]): row for row in last_commission_rates
        }

        commission_rates_data = await self._wb_parser.parse_commission_rates()
        commission_rates = []
        for rate in commission_rates_data.get("data").get("categories"):
            commission_rate = {
                "category_name": rate.get("name")
                if rate.get("name")
                else "Цифровые товары",
                "item_name": rate.get("subject"),
                "fbo_rate": str_to_float(rate.get("percent")),
                "fbs_rate": str_to_float(rate.get("percentFBS")),
                "china_rate": str_to_float(rate.get("percentChina")),
                "date": datetime.date.today(),
            }
            key = (
                commission_rate["category_name"],
                commission_rate["item_name"],
            )
            if (
                key not in last_commission_dict
                or last_commission_dict[key][3] != commission_rate["fbo_rate"]
                or last_commission_dict[key][4] != commission_rate["fbs_rate"]
                or last_commission_dict[key][5]
                != commission_rate["china_rate"]
            ):
                commission_rates.append(commission_rate)
        return commission_rates

    async def insert_commission_rates(self) -> None:
        """
        Вставка данных о коммисиях по категориям товаров
        """
        commission_rates = await self.get_commission_rates()
        if commission_rates:
            await self._db_client.insert_data(
                "wb_commission_rates", commission_rates
            )
        else:
            logger.info("Коммисии по категориям не изменились")

    async def get_acceptance_coefficients(
        self,
        date=datetime.date.today(),
    ) -> list[dict]:
        """
        Извлечение данных о коэфициенте приемки
        Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
        """
        acceptance_coefficients_data = (
            await self._wb_parser.parse_acceptance_coefficients(date)
        )
        acceptance_coefficients = []
        warehouses_dict = {}
        rows = await self._db_client.pool.fetch(
            "SELECT id, name FROM wb_warehouses"
        )
        for row in rows:
            warehouses_dict[row["name"]] = row["id"]
        for coefficient in acceptance_coefficients_data.get("result").get(
            "report"
        ):
            warehouse_name = coefficient.get("warehouseName")
            warehouse_id = warehouses_dict.get(warehouse_name)

            acceptance_coefficient = {
                "date": datetime.datetime.fromisoformat(
                    coefficient.get("date").rstrip("Z")
                ).date(),
                "acceptance_type": int(coefficient.get("acceptanceType")),
                "coefficient": int(coefficient.get("coefficient")),
                "warehouse_id_from_json": int(coefficient.get("warehouseID")),
                "warehouse_name": coefficient.get("warehouseName"),
                "warehouse_id": int(warehouse_id) if warehouse_id else None,
            }
            acceptance_coefficients.append(acceptance_coefficient)
        return acceptance_coefficients

    async def insert_acceptance_coefficients(
        self, date=datetime.date.today()
    ) -> None:
        """
        Вставка данных о коэфициентах приемки
        """
        acceptance_coefficients = await self.get_acceptance_coefficients(date)
        if acceptance_coefficients:
            await self._db_client.insert_update_data(
                "wb_acceptance_coefficients",
                acceptance_coefficients,
                conflict_target="wb_acceptance_coefficients_date_warehouse_id_acceptance_typ_key",
                update_fields=["coefficient"],
            )
        else:
            logger.info("Коэффициентов приемки на эту дату нет")

    async def get_return_tariffs(
        self, date=datetime.date.today()
    ) -> list[dict]:
        """
        Извлечение данных о ставках логистики по возвратам
        Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
        """
        return_tariffs_data = await self._wb_parser.return_tariffs(date)
        return_tariffs = []
        for warehouse in return_tariffs_data.get("data").get("warehouseList"):
            tariff = {
                "date": date,
                "warehouse_sort": int(warehouse.get("warehouseSort")),
                "warehouse_name": warehouse.get("warehouseName"),
                "delivery_dump_sup_office_expr": warehouse.get(
                    "deliveryDumpSupOfficeExpr"
                ),
                "delivery_dump_sup_office_base": str_to_float(
                    warehouse.get("deliveryDumpSupOfficeBase")
                ),
                "delivery_dump_sup_office_liter": str_to_float(
                    warehouse.get("deliveryDumpSupOfficeLiter")
                ),
                "delivery_dump_sup_courier_expr": warehouse.get(
                    "deliveryDumpSupCourierExpr"
                ),
                "delivery_dump_sup_courier_base": str_to_float(
                    warehouse.get("deliveryDumpSupCourierBase")
                ),
                "delivery_dump_sup_courier_liter": str_to_float(
                    warehouse.get("deliveryDumpSupCourierLiter")
                ),
                "delivery_dump_sup_return_expr": warehouse.get(
                    "deliveryDumpSupReturnExpr"
                ),
                "delivery_dump_kgt_office_expr": warehouse.get(
                    "deliveryDumpKgtOfficeExpr"
                ),
                "delivery_dump_kgt_office_base": str_to_float(
                    warehouse.get("deliveryDumpKgtOfficeBase")
                ),
                "delivery_dump_kgt_office_liter": str_to_float(
                    warehouse.get("deliveryDumpKgtOfficeLiter")
                ),
                "delivery_dump_kgt_return_expr": warehouse.get(
                    "deliveryDumpKgtReturnExpr"
                ),
                "delivery_dump_srg_office_expr": warehouse.get(
                    "deliveryDumpSrgOfficeExpr"
                ),
                "delivery_dump_srg_return_expr": warehouse.get(
                    "deliveryDumpSrgReturnExpr"
                ),
            }
            return_tariffs.append(tariff)

        return return_tariffs

    async def insert_return_tariffs(self, date=datetime.date.today()) -> None:
        """
        Вставка данных о ставках по возвратам
        """
        return_tariffs = await self.get_return_tariffs(date)
        if return_tariffs:
            await self._db_client.insert_data(
                "wb_return_tariffs", return_tariffs
            )
        else:
            logger.info("Тарифов возврата на эту дату нет")
