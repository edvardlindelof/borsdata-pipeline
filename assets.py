from functools import partial
from glob import glob

from dagster import (
    AssetIn,
    asset as _asset,
    multi_asset,
    AssetOut,
    StaticPartitionsDefinition,
    OpExecutionContext
)
from dagstermill import define_dagstermill_asset
import nbconvert
import nbformat
import numpy as np
import pandas as pd


SHEET_NAMES = 'Info Year R12 Quarter PriceDay PriceWeek PriceMonth'.split()
directory_partitions = StaticPartitionsDefinition(glob('borsdata-files/*'))
asset = partial(_asset, partitions_def=directory_partitions)


@asset(io_manager_key='borsdata_input_manager')
def files(context: OpExecutionContext):
    """Börsdata excel files"""
    return sorted(glob(f'{context.partition_key}/*.xlsx'))

@multi_asset(
    outs={
        f'raw_{sn.lower()}_sheets': AssetOut(description=f'"{sn}" sheets')
        for sn in SHEET_NAMES
    },
    partitions_def=directory_partitions
)
def sheets(files):
    return tuple(
        pd.concat([dfs[sn].assign(filename=fn) for fn, dfs in files.items()])
        for sn in SHEET_NAMES
    )

@asset
def company_info(raw_info_sheets: pd.DataFrame):
    """Ticker etc"""
    return pd.concat([
        df.loc[df.index[13:27], 'B':'C'].set_index('B').T.assign(filename=fn)
        for fn, df in raw_info_sheets.groupby('filename')
    ])

@asset
def financial_statements(raw_r12_sheets: pd.DataFrame):
    """Quarterly reported 12 month trailing financial figures"""
    return pd.concat([
        df.set_index('A').drop(index=np.nan).loc[:, 'C':'L'].T.assign(filename=fn)
        for fn, df in raw_r12_sheets.groupby('filename')
    ])

@asset
def monthly_prices(raw_pricemonth_sheets: pd.DataFrame):
    """Last open/high/low/close of previous months as well as the very latest"""
    df = raw_pricemonth_sheets
    return (
        df.loc[df['A'] != 'Date']
        .rename(columns=df.loc[0, 'A':'F'].to_dict())
        .sort_values(['filename', 'Date'])
    )

@asset
def daily_closes(raw_priceday_sheets: pd.DataFrame, company_info: pd.DataFrame):
    """Daily closes for all companies"""
    df = raw_priceday_sheets
    return (
        df.loc[df['A'] != 'Date']
        .rename(columns=df.loc[0, 'A':'F'].to_dict())
        .merge(company_info, 'left', 'filename')
        .pivot_table(index='Date', columns='Company', values='Closeprice')
        .reset_index()
    )

def define_notebook_html_asset(name, dagstermill_asset):
    def _compute_fn(notebook):
        html, _ = (
            nbconvert.HTMLExporter()
            .from_notebook_node(nbformat.reads(notebook.decode(), as_version=4))
        )
        return html
    return asset(
        name=name,
        io_manager_key='notebook_html_io_manager',
        ins={'notebook': AssetIn(dagstermill_asset.key)}
    )(
        _compute_fn
    )

daily_closeprice_plot_asset = define_dagstermill_asset(
    name='daily_closeprice_plot',
    notebook_path='notebooks/daily_closeprice_plot.ipynb',
    ins={'df': AssetIn('daily_closes')},
    partitions_def=directory_partitions
)

daily_closeprice_plot_html_asset = define_notebook_html_asset(
    name='daily_closeprice_plot_as_html',
    dagstermill_asset=daily_closeprice_plot_asset
)
