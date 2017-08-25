# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.hooks.jdbc_hook import JdbcHook
from airflow.operators.jdbc_operator import JdbcOperator
from airflow.utils.decorators import apply_defaults


class JdbcExportOperator(JdbcOperator):
    """
    Execute Jdbc Query and saves the output to a file.

    Uses an internmediary Pandas Dataframe.
    This does requires all the data loaded into a pandas dataframe,
    so a batch / row based alternative might be useful for larger datasets.

    :param sql: SQL query (template) to execute
    :type sql: str
    :param destination: Local Ouput file path (template)
    :type destination: str
    :param format: Output format - CSV or NEWLINE_DELIMITED_JSON
    :type format: str
    :param header: (CSV only)
    :type header: bool
    :param formatters: dict of callables to format each column by db name
    :type formatters: dict
    :param columns: callable to transform or dict mapping {old: new} names
    :type columns: callable or dict
    :param xcom_push_row_count: flag to push row count to xcom
    :type xcom_push_row_count: bool

    Other params defined in JdbcOperator
    """
    template_fields = ('sql', 'destination')

    __CSV = 'CSV'
    __JSON = 'NEWLINE_DELIMITED_JSON'
    __formats = {__CSV, __JSON}

    @apply_defaults
    def __init__(
            self, sql, destination, format='CSV',
            header=True, sep=',',
            formatters=None,
            columns=None,
            xcom_push_row_count=False,
            jdbc_conn_id='jdbc_default', autocommit=False,
            parameters=None,
            *args, **kwargs):
        super(JdbcExportOperator, self).__init__(sql=sql,
                                                 jdbc_conn_id=jdbc_conn_id,
                                                 autocommit=autocommit,
                                                 parameters=parameters,
                                                 *args,
                                                 **kwargs)
        self.destination = destination
        self.formatters = formatters or {}
        self.columns = columns or {}
        self.format = format
        self.xcom_push_row_count = xcom_push_row_count
        self.sep = sep
        self.header = header

        for col, fmtfun in self.formatters.items():
            if not callable(fmtfun):
                msg = "Object {} at index {} not callable".format(fmtfun, col)
                raise ValueError(msg)

        if not (callable(self.columns) or isinstance(self.columns, dict)):
            raise ValueError("columns not callable or dict")

        if self.format not in self.__formats:
            msg = "Format {} not in {}".format(format, self.__formats)
            raise ValueError(msg)

    def execute(self, context):
        logging.info('Exporting: ' + self.sql)
        logging.info('Destination: ' + self.destination)

        self.hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        df = self.hook.get_pandas_df(self.sql, self.parameters)
        self._write_df(df)

        if self.xcom_push_row_count:
            return df.shape[0]

    def _write_df(self, df):
        """Format and write the dataframe """

        for col in self.formatters:
            fmtfun = self.formatters[col]
            if col in df.columns:
                df[col] = df[col].apply(fmtfun)

        if callable(self.columns):
            df.columns = [self.columns(c) for c in df.columns]
        else:
            # noop with {}
            df = df.rename(columns=self.columns)

        # Call approriate function based on desired format
        if self.__CSV == self.format:
            df.to_csv(self.destination, index=False, header=self.header,
                      sep=self.sep)
        elif self.__JSON == self.format:
            with open(self.destination, 'w') as fout:
                lines = df.to_json(orient='records', lines=True)
                fout.write(lines)
        else:
            msg = "Couldn't write to format {}".format(self.format)
            logging.warn(msg)
