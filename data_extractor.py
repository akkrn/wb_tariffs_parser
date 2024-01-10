import datetime

from db_client import DBClient
from utils import str_to_float
from wb_delivery_parsing import WildberriesDelivery


async def get_weekly_rating(
    row_id: int, wb_delivery: WildberriesDelivery
) -> dict:
    """
    Извлечение данных о коэфициенте логистики и индексе локализации
    Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
    """
    weekly_rating_data = await wb_delivery.weekly_rating()
    weekly_rating = {
        "seller_id": row_id,
        "logistics_coefficient": weekly_rating_data["data"][
            "logisticAndStorage"
        ]["rating"],
        "localization_index": weekly_rating_data["data"]["localization"][
            "index"
        ],
        "date": datetime.date.today() - datetime.timedelta(days=0),
    }
    return weekly_rating


async def get_warehouse_tariffs(
    wb_delivery: WildberriesDelivery,
    db_client: DBClient,
    date=datetime.date.today(),
) -> list[dict]:
    """
    Извлечение данных о тарифах скаладов
    Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
    """
    warehouse_tariffs_data = await wb_delivery.warehouses_tariffs(date)
    warehouse_tariffs = []
    warehouses_dict = {}
    rows = await db_client.pool.fetch("SELECT id, name FROM wb_warehouses")
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
            "box_storage_base": str_to_float(warehouse.get("boxStorageBase")),
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
        }
        warehouse_tariffs.append(warehouse_data)
    return warehouse_tariffs


async def get_logistics_rates(
    wb_delivery: WildberriesDelivery, db_client: DBClient
) -> list[dict]:
    """
    Извлечение данных о ставках логистики по категориям товаров
    Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
    """
    logistics_rates_data = await wb_delivery.logistics_rates()
    category_dict = {}
    rows = await db_client.pool.fetch(
        "SELECT id, category_name, item_name FROM wb_categories"
    )
    for row in rows:
        category_dict[(row["category_name"], row["item_name"])] = row["id"]
    logistics_rates = []
    for rate in logistics_rates_data.get("data").get("categories"):
        category_name = rate.get("name")
        if (len(category_name)) == 0:
            category_name = None
        item_name = rate.get("subject")
        try:
            category_id = category_dict[(category_name, item_name)]
        except KeyError:
            continue
        logistics_rate = {
            "category_id": category_id,
            "fbo_rate": str_to_float(rate.get("percent")),
            "fbs_rate": str_to_float(rate.get("percentFBS")),
            "china_rate": str_to_float(rate.get("percentChina")),
            "date": datetime.date.today(),
        }
        logistics_rates.append(logistics_rate)
    return logistics_rates


async def get_acceptance_coefficients(
    wb_delivery: WildberriesDelivery,
    db_client: DBClient,
    date=datetime.date.today(),
) -> list[dict]:
    """
    Извлечение данных о коэфициенте приемки
    Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
    """
    acceptance_coefficients_data = await wb_delivery.acceptance_сoefficients(
        date
    )
    acceptance_coefficients = []
    warehouses_dict = {}
    rows = await db_client.pool.fetch("SELECT id, name FROM wb_warehouses")
    for row in rows:
        warehouses_dict[row["name"]] = row["id"]
    for coefficient in acceptance_coefficients_data.get("result").get(
        "report"
    ):
        warehouse_name = coefficient.get("warehouseName")
        warehouse_id = warehouses_dict.get(warehouse_name)
        if warehouse_id is None:
            continue
        acceptance_coefficient = {
            "date": datetime.datetime.fromisoformat(
                coefficient.get("date").rstrip("Z")
            ).date(),
            "acceptance_type": int(coefficient.get("acceptanceType")),
            "coefficient": int(coefficient.get("coefficient")),
            "warehouse_id_from_json": int(coefficient.get("warehouseID")),
            "warehouse_name": coefficient.get("warehouseName"),
            "warehouse_id": int(warehouse_id),
        }
        acceptance_coefficients.append(acceptance_coefficient)
    return acceptance_coefficients


async def get_return_tariffs(
    wb_delivery: WildberriesDelivery, date=datetime.date.today()
) -> list[dict]:
    """
    Извлечение данных о ставках логистики по возвратам
    Порядок записи данных важен, так как при вставке в бд поля будут в том же порядке
    """
    return_tariffs_data = await wb_delivery.return_tariffs(date)
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
