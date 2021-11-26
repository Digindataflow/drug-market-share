from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.absolute()


PIPELINE_NAME = "sales_crm"

SALES_DATA_PATH = ROOT_DIR.joinpath("data/landing/sales/")
CRM_DATA_PATH = ROOT_DIR.joinpath("data/landing/crm/crm_data.csv")
SALES_CRM_DATA_PATH = ROOT_DIR.joinpath(
    "data/production/sales_crm/market_share_event_sum.csv"
)

DECIMAL_DIGITS = 2

SALES_WINDOW_SIZE_WEIGHTS = {2: [], 3: []}
CRM_WINDOW_SIZE_WEIGHTS = {2: [0.3, 0.7], 3: [0.25, 0.25, 0.5]}
