from datetime import datetime
from pathlib import Path
import tempfile

from data_logger.file_logger import FileLogger

def test_file_logger_new_dir():
    with tempfile.TemporaryDirectory() as tmpdirname:
        fl1 = FileLogger('sample', tmpdirname, columns=['A', 'B', 'C'])
        assert fl1.type == 'sample'
        assert fl1.root_dir == Path(tmpdirname)
        dt1 = datetime(2015, 1, 2, 3, 4, 5)
        dt2 = datetime(2015, 1, 2, 4, 5, 6)
        dt3 = datetime(2015, 1, 3, 1, 2, 3)
        dt4 = datetime(2015, 1, 3, 1, 5, 3)
        fl1.add_row_by_list(dt1, [1., 2., 3.])
        assert (Path(tmpdirname) / '2015' / '01').is_dir()
        assert (Path(tmpdirname) / '2015' / '01' / 'sample_2015_01_02.csv').is_file()
        assert not (Path(tmpdirname) / '2015' / '01' / 'sample_2015_01_03.csv').is_file()
        fl1.add_row_by_list(dt2, [3., 6., 9.])
        fl1.add_row_by_list(dt3, [10., 20., 30.])
        fl1.add_row_by_list(dt4, [30., 60., 90.])
        assert (Path(tmpdirname) / '2015' / '01' / 'sample_2015_01_03.csv').is_file()

        assert fl1.daily_min_by_column(dt1) == [1., 2., 3.]
        assert fl1.daily_max_by_column(dt1) == [3., 6., 9.]
        assert fl1.daily_mean_by_column(dt1) == [2., 4., 6.]
        assert fl1.daily_min_by_column(dt3) == [10., 20., 30.]
        assert fl1.daily_max_by_column(dt3) == [30., 60., 90.]
        assert fl1.daily_mean_by_column(dt3) == [20., 40., 60.]
        assert fl1.daily_min_by_column(dt1) == [1., 2., 3.]
        assert fl1.daily_max_by_column(dt1) == [3., 6., 9.]
        assert fl1.daily_mean_by_column(dt1) == [2., 4., 6.]

def test_file_logger_new_dir_by_col():
    with tempfile.TemporaryDirectory() as tmpdirname:
        fl = FileLogger('sample', tmpdirname, columns=['A', 'B', 'C'])
        dt1 = datetime(2017, 1, 2, 3, 4, 5)
        dt2 = datetime(2017, 1, 2, 4, 5, 6)
        row = {}
        row['C'] = 3.
        row['A'] = 1.
        row['B'] = 2.
        fl.add_row_by_dict(dt1, row)

        assert fl.daily_min_by_column(dt1) == [1., 2., 3.]
        assert fl.daily_max_by_column(dt1) == [1., 2., 3.]
        assert fl.daily_mean_by_column(dt1) == [1., 2., 3.]

def test_file_logger_new_dir_no_autoflush():
    with tempfile.TemporaryDirectory() as tmpdirname:
        fl = FileLogger('sample', tmpdirname, columns=['A', 'B', 'C'],
                         auto_flush=False)
        dt = datetime(2013, 12, 31, 23, 59, 59)
        fl.add_row_by_list(dt, [1., 2., 3.])
        assert (Path(tmpdirname) / '2013' / '12').is_dir()
        assert not (Path(tmpdirname) / '2013' / '12' / 'sample_2013_12_31.csv').exists()
        fl.flush()
        assert (Path(tmpdirname) / '2013' / '12' / 'sample_2013_12_31.csv').is_file()
        fl.flush()
        assert (Path(tmpdirname) / '2013' / '12' / 'sample_2013_12_31.csv').is_file()

def test_file_logger_old_dir_not_exist():
    fl = FileLogger('sample', 'test_files/file_logger')
    dt = datetime(2021, 6, 3, 0, 0, 0)
    assert fl.daily_min_by_column(dt) is None
    assert fl.daily_max_by_column(dt) is None
    assert fl.daily_mean_by_column(dt) is None

def test_file_logger_old_dir():
    fl = FileLogger('existing', 'test_files/file_logger')
    dt = datetime(2021, 6, 3, 0, 0, 0)
    fl._read_data_if_necessary(dt)
    assert fl.daily_min_by_column(dt) == [0.1, 0.2, 0.3]
    assert fl.daily_max_by_column(dt) == [0.7, 0.8, 0.9]
    assert fl.daily_mean_by_column(dt) == [0.4, 0.5, 0.6]
