import os

file_name = os.path.basename(__file__)

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from datetime import datetime, timedelta
from src.constant import cross_bridge_raw
from src.utils import (
    MySQLReader,
    get_job_info,
    S3Handler,
    repair_glue_partitions,
)

### set parameters
job = get_job_info(file_name)
data_layer = job["data_layer"]
schema_name = file_name.split("dl_")[1].split(".py")[0]
start_date = job["start_date"]
end_date = job["end_date"]
batch_bdate = job["batch_bdate"]
###


def main(spark):
    for dataset in cross_bridge_raw:
        db_reader = MySQLReader(spark, dataset["db_config"])
        if dataset["Table"] == "history":
            global start_date
            date_obj = datetime.strptime(start_date, "%Y-%m-%d")
            two_days_ago = date_obj - timedelta(days=1)
            start_date = two_days_ago.strftime("%Y-%m-%d")

        print(dataset["Table"])
        df = db_reader.read_table(
            table_name=dataset["Table"],
            select_field=dataset["SelectField"],
            time_field=dataset.get("TimeField", None),
            start_date=start_date,
            end_date=end_date,
            mode=dataset.get("Mode", "append"),
        )
        if isinstance(df, DataFrame) and df.count() > 0:
            df.limit(3).show()

            s3_writer = S3Handler()
            if dataset["Table"] == "history":
                dataset["Table"] = "bridge_history"

            try:
                s3_writer.write_table(
                    df=df,
                    data_layer=data_layer,
                    schema_name=schema_name,
                    table_name=dataset["Table"],
                    partition_cols=dataset.get("PartitionCols", ["dt_utc"]),
                    format=dataset.get("Format", "parquet"),
                    mode=dataset.get("Mode", "append"),
                )
                print(f">>> S3 write SUCCESS \n")
            except Exception as e:
                print(f">>> S3 write FAILED: {e}")

            repair_glue_partitions(spark, schema_name, data_layer, dataset["Table"])
            s3_writer.table_manager(
                spark,
                schema_name,
                dataset["Table"],
                batch_bdate,
                start_date,
                end_date,
                status=0,
            )
            print(f">>> Table Manager SUCCESS \n")

    # spark.stop()
