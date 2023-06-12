from typing import Any

from dagster import OutputContext, io_manager, InitResourceContext, _check
from dagster._core.execution.context.input import InputContext
from dagster._core.execution.context.output import OutputContext
from dagster._core.storage.fs_io_manager import PickledObjectFilesystemIOManager
import pandas as pd
from upath import UPath


class PandasCSVIOManager(PickledObjectFilesystemIOManager):

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath):
        obj.to_csv(path.with_suffix('.csv'), index=False)
    
    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        return pd.read_csv(path.with_suffix('.csv'))
    
    def get_metadata(self, context: OutputContext, obj: Any): # -> Dict[str, MetadataValue[PackableValue]]:
        description_df = obj.describe()
        type_mapping = {
            col: type_ if type_ in [int, float] else str
            for col, type_ in description_df.dtypes.items()
        }
        return description_df.astype(type_mapping).to_dict(orient='tight')

@io_manager
def pandas_csv_io_manager(context: InitResourceContext):
    return PandasCSVIOManager(  # set default path same way as fs_io_manager
        base_dir=_check.not_none(context.instance).storage_directory()
    )


class ExcelInputManager(PickledObjectFilesystemIOManager):

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath):
        with open(path, "w") as f:
            f.write('\n'.join(obj))

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        with open(path, "r") as f:
            filepaths = f.read().split('\n')
        return {
            fp: {
                sheet_name: df.rename(columns=self._colnum2colname)
                for sheet_name, df in self._read_all_sheets(fp, header=None).items()
            }
            for fp in filepaths
        }

    @staticmethod
    def _read_all_sheets(path, **read_kwargs):
        excel_file = pd.ExcelFile(path)
        return pd.read_excel(excel_file, excel_file.sheet_names, **read_kwargs)

    @staticmethod
    def _colnum2colname(i):
        if i > 25:
            raise ValueError('i > 25')
        return chr(65 + i)

    def get_metadata(self, context: OutputContext, obj: Any): # -> Dict[str, MetadataValue[PackableValue]]:
        return {'filenames': obj}

@io_manager
def excel_input_manager(context: InitResourceContext):
    return ExcelInputManager(  # set default path same way as fs_io_manager
        base_dir=_check.not_none(context.instance).storage_directory()
    )

