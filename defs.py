from dagster import Definitions, load_assets_from_modules
from dagstermill import local_output_notebook_io_manager

from io_managers import excel_input_manager, pandas_csv_io_manager
import assets


defs = Definitions(
    load_assets_from_modules([assets]),
    resources={
        'io_manager': pandas_csv_io_manager,
        'borsdata_input_manager': excel_input_manager,
        'output_notebook_io_manager': local_output_notebook_io_manager
    }
)
