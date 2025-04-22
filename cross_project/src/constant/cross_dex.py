from src.utils import DBConfig, get_secret
from ..base import BaseConfig

CROSS_DEX_DB_CONFIG = DBConfig(
    host=BaseConfig.DEX_DB_HOST,
    port=3306,
    dbname="dex_cross",
    user=eval(get_secret(BaseConfig.DEX_DB_INFO)).get("username"),
    password=eval(get_secret(BaseConfig.DEX_DB_INFO)).get("password"),
)

cross_dex_raw = [
    {
        "Service": "cross_dex",
        "Table": "close_orders",
        "TimeField": "closed_at",
        "SelectField": "idx, lower(concat('0x',HEX(pair_address))) AS pair_address, order_id, lower(concat('0x',HEX(owner))) AS owner, order_side, price, amount, volume, filled, filled_volume, price_usd_at, volume_usd_at, filled_usd_at, created_at, created_block, lower(concat('0x',HEX(created_hash))) AS created_hash, closed_type, closed_at, closed_block, lower(concat('0x',HEX(closed_hash))) AS closed_hash",
        "db_config": CROSS_DEX_DB_CONFIG,
    },
    {
        "Service": "cross_dex",
        "Table": "fee_collectors t1 left outer join (select distinct block, time_utc from match_histories) t2 on t1.block = t2.block",
        "TimeField": "time_utc",
        "SelectField": "idx, lower(concat('0x',HEX(pair_address))) AS pair_address, order_id, lower(concat('0x',HEX(owner))) AS owner, gross_amount, lower(concat('0x',HEX(recipient))) AS recipient, fee_bps, fee, fee_usd_at, net_amount, t1.block AS block, lower(concat('0x',HEX(hash))) AS hash, time_utc",
        "db_config": CROSS_DEX_DB_CONFIG,
    },
    {
        "Service": "cross_dex",
        "Table": "user_orders",
        "Mode": "overwrite",
        "TimeField": "created_at",
        "SelectField": "idx, lower(concat('0x',HEX(pair_address))) AS pair_address, order_id, lower(concat('0x',HEX(owner))) AS owner, order_side, price, amount, volume, filled, filled_volume, price_usd_at, volume_usd_at, filled_usd_at, created_at, created_block, lower(concat('0x',HEX(created_hash))) AS created_hash, closed_type, closed_at, closed_block, lower(concat('0x',HEX(closed_hash))) AS closed_hash",
        "db_config": CROSS_DEX_DB_CONFIG,
    },
    {
        "Service": "cross_dex",
        "Table": "match_histories",
        "TimeField": "time_utc",
        "SelectField": "idx, lower(concat('0x',HEX(pair_address))) AS pair_address, order_id, lower(concat('0x',HEX(owner))) AS owner, order_side, is_taker, price, amount, volume, fee, price_usd_at, volume_usd_at, fee_usd_at, time_utc, block, lower(concat('0x',HEX(hash))) AS hash",
        "db_config": CROSS_DEX_DB_CONFIG,
    },
    {
        "Service": "cross_dex",
        "Table": "pair_infos",
        "Mode": "overwrite",
        "PartitionCols": [],
        "SelectField": "lower(concat('0x',HEX(pair_address))) AS pair_address, pair_name, lower(concat('0x',HEX(quote_address))) AS quote_address, lower(concat('0x',HEX(base_address))) AS base_address, quote_decimals, base_decimals,open_time, open_price",
        "db_config": CROSS_DEX_DB_CONFIG,
    },
]
