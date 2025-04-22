from src.constant import cross_wallet_raw, cross_dex_raw
from src.validators import CountValidator
from src.utils import SlackMessenger


def main(spark):
    service_list = ["cross_dex_raw"]
    suc_count = 0
    total_validation_list = {}

    for service in service_list:
        for dataset in eval(service):
            count_validator = CountValidator(spark, dataset)
            cnt = count_validator.validate_count()
            total_validation_list[
                f"{cnt['service_name']}.{cnt['table_name'].split(' ')[0]}"
            ] = [
                cnt["db_count"],
                cnt["s3_count"],
                cnt["catalog_count"],
            ]

            if cnt["db_count"] == cnt["s3_count"] == cnt["catalog_count"]:
                suc_count += 1
            else:
                if cnt["table_name"] in ("dl_user_orders"):  # 예외 케이스
                    if (
                        cnt["s3_count"] == cnt["catalog_count"]
                        and cnt["db_count"] - cnt["s3_count"] < 30
                    ):
                        suc_count += 1
            print(
                f"{cnt['service_name']}, {cnt['table_name']}  db_count: {cnt['db_count']}, s3_count: {cnt['s3_count']}, catalog_count: {cnt['catalog_count']}"
            )

    comments = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{key} {f'✅ *DB: {values[0]} S3: {values[1]} Catalog: {values[2]}*' if values[0] == values[1] == values[2] else f'❌ *DB: {values[0]}, S3: {values[1]}, Catalog: {values[2]}*'}",
            },
        }
        for key, values in total_validation_list.items()
    ]

    # has_error = any(
    #     values[0] != values[1] or values[0] != values[2]
    #     for values in total_validation_list.values()
    # )
    has_error = False if suc_count == len(total_validation_list) else True
    color = "#ff0000" if has_error else "#36a64f"

    try:
        if suc_count == len(total_validation_list):
            print("All counts are correct")
        else:
            raise Exception("Some counts are incorrect")
    finally:
        pass
        messenger = SlackMessenger()
        messenger.send_slack(
            text="🔍 *DL COUNT CHECK RESULTS*",
            block_message=comments,
            color=color,
        )

    # spark.stop()
