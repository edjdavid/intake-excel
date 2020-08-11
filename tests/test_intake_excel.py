import intake
import pandas as pd
import numpy as np
import dask.dataframe as dd
import pytest
import tempfile
import shutil
import os
from intake_excel import ExcelSource

# pytest imports this package last, so plugin is not auto-added
intake.registry['excel'] = ExcelSource

df = pd.DataFrame(np.random.randint(1, 10, (10, 3)), columns=list('abc'))
df2 = pd.DataFrame(np.random.randint(1, 10, (10, 3)), columns=list('abc'))

@pytest.fixture(scope='module')
def temp_files():
    d = tempfile.mkdtemp()
    fnames = []
    i = 0
    for _df in [df, df2]:
        f, fname = tempfile.mkstemp(prefix=f'{i}_', suffix='.xlsx', dir=d)
        i += 1
        _df.to_excel(fname, index=False)
        fnames.append(fname)
    try:
        yield fnames
    finally:
        if os.path.isdir(d):
            shutil.rmtree(d)

def test_fixture(temp_files):
    print(temp_files)
    assert df.equals(pd.read_excel(temp_files[0], header=0))
    assert df2.equals(pd.read_excel(temp_files[1], header=0))

def test_load_single(temp_files):
    isrc = intake.open_excel(temp_files[0], excel_kwargs=dict(header=0))
    assert df.equals(isrc.read())

def test_load_glob(temp_files):
    d = os.path.dirname(temp_files[0])
    isrc = intake.open_excel(f'{d}/*.xlsx', excel_kwargs=dict(header=0))
    intake_df = isrc.read()
    pd_concat = pd.concat([df, df2])
    np.testing.assert_equal(pd_concat.index.values, intake_df.index.values)
    np.testing.assert_equal(pd_concat.values, intake_df.values)
    np.testing.assert_equal(pd_concat.columns.values, intake_df.columns.values)
    pd_concat.equals(intake_df)

def test_load_dask(temp_files):
    d = os.path.dirname(temp_files[0])
    isrc = intake.open_excel(f'{d}/*.xlsx', excel_kwargs=dict(header=0))
    assert isinstance(isrc.to_dask(), dd.DataFrame)
    pd_concat = pd.concat([df, df2])
    assert pd_concat.equals(isrc.to_dask().compute())
