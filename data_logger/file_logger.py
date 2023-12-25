import datetime
from pathlib import Path

import pandas as pd

class FileLogger:
    def __init__(self, type, root_dir, columns=None, auto_flush=True):
        self._type = str(type)
        self._root_dir = Path(root_dir).absolute()
        if columns is None:
            columns = []
        self._auto_flush = auto_flush
        self._dirty = False
        self._columns = columns
        self._dates = []
        self._current_date = None
        self._current_path = None
        self._current_index = []
        self._current_data = []
        self._current_df = None

    @staticmethod
    def _date_tuple(dt):
        return (dt.year, dt.month, dt.day)

    @property
    def type(self):
        return self._type

    @property
    def root_dir(self):
        return self._root_dir

    def path_for_date(self, dt):
        y, m, d = self._date_tuple(dt)
        filename = f'{self._type}_{y:04d}_{m:02d}_{d:02d}.csv'
        path = self._root_dir / f'{y:4d}' / f'{m:02d}'
        path.mkdir(parents=True, exist_ok=True)
        return path / filename

    def _read_data_if_necessary(self, dt):
        new_date = self._date_tuple(dt)
        if self._current_date == new_date:
            return
        self._current_date = new_date
        self._current_path = self.path_for_date(dt)
        self._current_index = []
        self._current_data = []
        self._current_df = None
        if self._current_path.exists():
            self._current_df = pd.read_csv(self._current_path, index_col=0,
                                           parse_dates=True)
            self._current_data = self._current_df.values.tolist()
            self._current_index = self._current_df.index.values.tolist()
            self._columns = self._current_df.values.tolist()

    def flush(self):
        if self._current_path is None or not self._dirty:
            return
        if self._current_df is None:
            self._current_df = pd.DataFrame(self._current_data, self._current_index)
            self._current_df.index.name = 'DateTime'
        self._current_df.to_csv(self._current_path)
        self._dirty = False

    def add_row_by_list(self, dt, row):
        self._read_data_if_necessary(dt)
        self._current_index.append(dt)
        self._current_data.append(row)
        self._current_df = None
        self._dirty = True
        if self._auto_flush:
            self.flush()

    def add_row_by_dict(self, dt, d):
        row = []
        for col_name in self._columns:
            row.append(d[col_name])
        self.add_row_by_list(dt, row)

    def daily_min_by_column(self, dt):
        self._read_data_if_necessary(dt)
        if self._current_df is None:
            return None
        return self._current_df.min().tolist()

    def daily_max_by_column(self, dt):
        self._read_data_if_necessary(dt)
        if self._current_df is None:
            return None
        return self._current_df.max().tolist()

    def daily_mean_by_column(self, dt):
        self._read_data_if_necessary(dt)
        if self._current_df is None:
            return None
        return self._current_df.mean().tolist()
