import logging
import os

import pandas as pd
from errors import ValidationError
from process_data import MarketShareProcess, MarketEventProcess
from connect_data import DataFrameValidator, read_json_file, read_csv_file
import settings

# assumption: schema for each source is read from a database
sales_data_schema = {
    "acct_id": {"type": str},
    "product_name": {
        "type": str,
        "choices": ["Globberin", "Vorbulon", "Snaffleflax", "Beeblizox"],
        "value_mapping": {
            "Globberin": ["Globbrin", " Globberin"],
            "Vorbulon": ["vorbulon."],
            "Snaffleflax": ["Snafulopromazide-b (Snaffleflax)"],
            "Beeblizox": ["Beebliz%C3%B6x"],
        },
    },
    "date": {"type": "date"},
    "unit_sales": {"type": int},
    "created_at": {"type": "date"},
}

crm_data_schema = {
    "acct_id": {"type": str},
    "event_type": {
        "type": str,
        "choices": ["f2f", "group call", "workplace event"],
    },
    "date": {"type": "date"},
}

# assumption: logging is saved to a file for monitoring
logger = logging.getLogger(settings.PIPELINE_NAME)


# assumption:
# 1. error is raised to fail whole batch process and people will get alert
# 2. pipeline activity metadata is recorded: status, duration, total rows etc.
# 3. pipeline setting up metadata is read from database, including
# source, destination, schema id
# 4. missing data, outlier in data, duplicates in data are not checked
# 5. failed data is saved to failing area

# further data: evaluation on online shop
def sales_to_market_share_pipeline() -> pd.DataFrame:
    """read sales data, transform to market share data
    """
    validator = DataFrameValidator()
    sales_data = []
    for file_path in os.listdir(settings.SALES_DATA_PATH):
        if not file_path.endswith(".json"):
            msg = "Sales data should be json files: %s" % file_path
            logger.error(msg)
            raise ValueError(msg)

        data = read_json_file(os.path.join(settings.SALES_DATA_PATH, file_path))

        try:
            data = validator.validate(sales_data_schema, data)
        except ValidationError as e:
            logger.error(e)
            raise

        sales_data.append(data)
    sales_data = pd.concat(sales_data, axis=0)

    market_share_process = MarketShareProcess(settings.SALES_WINDOW_SIZE_WEIGHTS)
    market_share = market_share_process.process(sales_data)
    return market_share


def crm_to_event_data_pipeline() -> pd.DataFrame:
    """read crm data, transform to event data
    """
    crm_data = read_csv_file(settings.CRM_DATA_PATH)
    validator = DataFrameValidator()

    try:
        crm_data = validator.validate(crm_data_schema, crm_data)
    except ValidationError as e:
        logger.error(e)
        raise

    market_event_process = MarketEventProcess(settings.CRM_WINDOW_SIZE_WEIGHTS)
    event_data = market_event_process.process(crm_data)
    return event_data


def pipeline() -> pd.DataFrame:
    """run crm and sales pipeline and merge two datasets
    """
    market_share = sales_to_market_share_pipeline()
    event_data = crm_to_event_data_pipeline()
    data = pd.merge(
        market_share, event_data, how="outer", left_index=True, right_index=True
    )
    data = data.reset_index(level=0)
    data.to_csv(settings.SALES_CRM_DATA_PATH)
    return data


if __name__ == "__main__":
    pipeline()
