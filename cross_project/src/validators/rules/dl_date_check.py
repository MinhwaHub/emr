from src.constant import cross_wallet_raw, cross_dex_raw
from src.validators import DateValidator
from src.utils import SlackMessenger, get_date


start_date, end_date = get_date()
service_list = ["cross_dex_raw"]
total_validation_list = {}


def main(spark):
    for service in service_list:
        for dataset in eval(service):
            if "TimeField" in dataset:
                date_validator = DateValidator(spark, dataset)
                dtv = date_validator.validate_count()
                total_validation_list[f"{dtv['service_name']}.{dtv['table_name']}"] = (
                    dtv["db_max_bdate"]
                )
            else:
                pass

    comments = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{key}: *{values}*",
            },
        }
        for key, values in total_validation_list.items()
    ]

    messenger = SlackMessenger()
    messenger.send_slack(
        text=f"🔍 *DB MAX BDATE CHECK RESULTS [{start_date} - {end_date}]*",
        block_message=comments,
    )

    # spark.stop()
