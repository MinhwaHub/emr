from src.utils import DBConfig, get_secret
from ..base import BaseConfig

CROSS_WALLET_DB_CONFIG = DBConfig(
    host=BaseConfig.WALLET_DB_HOST,
    port=3306,
    dbname="wallet",
    user=eval(get_secret(BaseConfig.WALLET_DB_INFO)).get("username"),
    password=eval(get_secret(BaseConfig.WALLET_DB_INFO)).get("password"),
)

cross_wallet_raw = [
    {
        "Service": "cross_wallet",
        "Table": "token",
        "PartitionCols": [],
        "Mode": "overwrite",
        "SelectField": "`_id`, chain_id, lower(concat('0x',HEX(address))) as address, name, symbol, decimals, category, required, enabled, created_at, updated_at",
        "db_config": CROSS_WALLET_DB_CONFIG,
    },
    {
        "Service": "cross_wallet",
        "Table": "token_price",
        "TimeField": "updated_at",
        "Query": "MINUTE(from_unixtime(updated_at)) = 0",
        "SelectField": "`_id`, chain_id, lower(concat('0x',HEX(address))) as address, price as price_usd, interval_flags, updated_at",
        "db_config": CROSS_WALLET_DB_CONFIG,
    },
]
