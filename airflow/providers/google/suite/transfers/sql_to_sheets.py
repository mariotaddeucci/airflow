# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


import datetime
import decimal
from contextlib import closing
from typing import Any, Iterable, Mapping, Optional, Sequence, Union

from airflow.operators.sql import BaseSQLOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class SQLToGoogleSheetsOperator(BaseSQLOperator):
    """
    Copy data from SQL results to provided Google Spreadsheet.

    :param sql: The SQL to execute.
    :type sql: str
    :param conn_id: the connection ID used to connect to the database.
    :type sql_conn_id: str
    :param parameters: The parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite the defined one in connection
    :type database: str
    :param spreadsheet_id: The Google Sheet ID to interact with.
    :type spreadsheet_id: str
    :param spreadsheet_range: The A1 notation of the values to retrieve.
    :type spreadsheet_range: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = [
        "sql",
        "spreadsheet_id",
        "spreadsheet_range",
        "impersonation_chain",
    ]

    template_ext = (".sql",)
    ui_color = "#a0e08c"

    def __init__(
        self,
        *,
        sql: str,
        spreadsheet_id: str,
        sql_conn_id: str = None,
        database: str = None,
        spreadsheet_range: str = "Sheet1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sql = sql
        self.conn_id = sql_conn_id
        self.database = database
        self.parameters = parameters
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def _data_prep(self, cursor):
        for row in cursor:
            for item in row:
                if type(item) is datetime.date:
                    item = item.strftime("%Y-%m-%d")
                elif type(item) is datetime.datetime:
                    item = item.strftime("%Y-%m-%d %H:%M:%S")
                elif type(item) is decimal.Decimal:
                    item = float(item)
                yield item

    def _get_data(self):
        hook = self.get_db_hook()
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                self.log.info("Executing query")
                if self.parameters is not None:
                    cur.execute(self.sql, self.parameters)
                else:
                    cur.execute(self.sql)

                yield [field[0] for field in cur.description]
                yield from self._data_prep(cur.fetchall())

    def execute(self, context: Any) -> None:
        self.log.info("Getting data")
        values = list(self._get_data())

        self.log.info("Connecting to Google")
        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info(f"Uploading data to https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}")

        sheet_hook.update_values(
            spreadsheet_id=self.spreadsheet_id,
            range_=self.spreadsheet_range,
            values=values,
        )
