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


import abc
import datetime
import decimal
import itertools
from typing import Any, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class SQLToGoogleSheetsOperator(BaseOperator):
    """
    Copy data from SQL to provided Google Spreadsheet.

    :param sql: The SQL to execute.
    :type sql: str
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
        'sql',
        "spreadsheet_id",
        "spreadsheet_range",
        "impersonation_chain",
    ]

    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    def __init__(
        self,
        *,
        sql: str,
        spreadsheet_id: str,
        spreadsheet_range: str = "Sheet1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def _data_prep(cursor):
        for row in cursor:
            for item in row:
                if type(item) is datetime.date:
                    item = item.strftime('%Y-%m-%d')
                elif type(item) is datetime.datetime:
                    item = item.strftime('%Y-%m-%d %H:%M:%S')
                elif type(item) is decimal.Decimal:
                    item = float(item)
                yield item

    @abc.abstractmethod
    def query(self):
        """Execute DBAPI query."""

    def execute(self, context: Any) -> None:
        self.log.info("Executing query")
        cursor = self.query()

        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        columns = [field[0] for field in cursor.description]
        values = itertools.chain(columns, self._data_prep(cursor))
        self.log.info(f"Uploading data to https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}")
        sheet_hook.update_values(
            spreadsheet_id=self.spreadsheet_id,
            range_=self.spreadsheet_range,
            values=values,
        )
