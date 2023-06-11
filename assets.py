from glob import glob

from dagster import asset, multi_asset, AssetOut
import numpy as np
import pandas as pd


SHEET_NAMES = 'Info Year R12 Quarter PriceDay PriceWeek PriceMonth'.split()


@asset(io_manager_key='borsdata_input_manager')
def files():
    """Börsdata excel files"""
    return sorted(glob('data/*.xlsx'))

@multi_asset(
    outs={
        f'raw_{sn.lower()}_sheets': AssetOut(description=f'"{sn}" sheets')
        for sn in SHEET_NAMES
    }
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

