import assets
from assets import SHEET_NAMES
from dagster import Definitions, load_assets_from_modules
from io_managers import excel_input_manager, pandas_csv_io_manager


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    resources={
        'io_manager': pandas_csv_io_manager,
        'borsdata_input_manager': excel_input_manager.configured(
            {'sheet_names': SHEET_NAMES}
        )
    }
)

