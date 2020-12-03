from glob import glob
import pandas as pd
import dask.dataframe as dd
from dask import delayed
from intake.source.base import DataSource, Schema


class ExcelSource(DataSource):
    """Read CSV files into dataframes

    Prototype of sources reading dataframe data

    """
    name = 'excel'
    version = '0.1.1'
    container = 'dataframe'
    partition_access = True

    def __init__(self, urlpath, excel_kwargs=None, metadata=None,
                 storage_options=None):
        """
        Parameters
        ----------
        urlpath : str or iterable, location of data
            A local path, may include glob wildcards or format pattern strings.
            ToDo: remote paths like s3://
            Some examples:

            - ``{{ CATALOG_DIR }}data/precipitation.xls``
            - ``{{ CATALOG_DIR }}data/precipitation_{date:%Y-%m-%d}.xlsx``
        excel_kwargs : dict
            Any further arguments to pass to Pandas' read_excel
        storage_options : dict
            Any parameters that need to be passed to the remote data backend,
            such as credentials.
        """
        self.urlpath = urlpath
        self._storage_options = storage_options
        self._excel_kwargs = excel_kwargs or {}
        self._dataframe = None

        super(ExcelSource, self).__init__(metadata=metadata)

    def _open_dataset(self):
        """Open dataset using dask and use pattern fields to set new columns
           ToDo: remote file source
        """
        # sort glob to have some control over output
        filenames = sorted(glob(self.urlpath))
        # glob returns empty on remote protocols
        if len(filenames) == 0:
            filenames = [self.urlpath]
        parts = [delayed(pd.read_excel)(fname, **self._excel_kwargs)
                 for fname in filenames]
        self._dataframe = dd.from_delayed(parts)

    def _get_schema(self):
        if self._dataframe is None:
            self._open_dataset()

        dtypes = self._dataframe._meta.dtypes.to_dict()
        dtypes = {n: str(t) for (n, t) in dtypes.items()}
        return Schema(datashape=None,
                      dtype=dtypes,
                      shape=(None, len(dtypes)),
                      npartitions=self._dataframe.npartitions,
                      extra_metadata={}
                     )

    def _get_partition(self, i):
        self._get_schema()
        return self._dataframe.get_partition(i).compute()

    def read(self):
        self._get_schema()
        return self._dataframe.compute()

    def to_dask(self):
        self._get_schema()
        return self._dataframe

    def to_spark(self):
        return NotImplementedError

    def _close(self):
        self._dataframe = None
