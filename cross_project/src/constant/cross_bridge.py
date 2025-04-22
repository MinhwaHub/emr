from src.utils import DBConfig, get_secret
from ..base import BaseConfig

CROSS_DEX_DB_CONFIG = DBConfig(
    host=BaseConfig.DEX_DB_HOST,
    port=3306,
    dbname="bridge",
    user=eval(get_secret(BaseConfig.DEX_DB_INFO)).get("username"),
    password=eval(get_secret(BaseConfig.DEX_DB_INFO)).get("password"),
)

cross_bridge_raw = [
    # {
    #     "Service": "cross_chain",
    #     "Table": "bridge_initiated",
    #     "TimeField": "time",
    #     "SelectField": "_id, chain_id, remote_chain_id, `index`, lower(concat('0x', hex(local_token))) as local_token, lower(concat('0x', hex(remote_token))) as remote_token, lower(concat('0x',hex(`from`))) as `from`,  lower(concat('0x', hex(`to`))) as `to`, value, exchange_fee, network_fee, extra_data, time,  lower(concat('0x', hex(tx_hash)))  as tx_hash",
    #     "db_config": CROSS_DEX_DB_CONFIG,
    # },
    {
        "Service": "cross_chain",
        "Table": "history",
        "TimeField": "initiated_time",
        "SelectField": "_id, from_chain_id, to_chain_id, `index`, lower(concat('0x', hex(from_token))) as from_token, lower(concat('0x', hex(to_token))) as to_token, lower(concat('0x', hex(`from`))) as `from`, lower(concat('0x', hex(`to`))) as `to`, value, exchange_fee, network_fee, lower(concat('0x', hex(`extra_data`))) as `extra_data`, lower(concat('0x', hex(initiated_tx_hash))) as initiated_tx_hash, lower(concat('0x', hex(finalized_tx_hash))) as finalized_tx_hash, initiated_time, finalized_time, status",
        "db_config": CROSS_DEX_DB_CONFIG,
    },
]
