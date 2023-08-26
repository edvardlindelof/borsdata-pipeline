from dagster import Definitions, load_assets_from_modules
from dagster import AssetIn
from dagstermill import define_dagstermill_asset
from dagstermill import local_output_notebook_io_manager

from io_managers import excel_input_manager, pandas_csv_io_manager
import assets


defs = Definitions(
    assets=load_assets_from_modules([assets]) + [
    define_dagstermill_asset(  # TODO move to better place
        name='daily_closeprice_plot',
        notebook_path='notebooks/daily_closeprice_plot.ipynb',
        ins={'df': AssetIn('daily_closes')}
    )],
    resources={
        'io_manager': pandas_csv_io_manager,
        'borsdata_input_manager': excel_input_manager,
        'output_notebook_io_manager': local_output_notebook_io_manager
    }
)
