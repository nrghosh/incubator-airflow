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

from impala.dbapi import connect
from impala.util import as_pandas

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class ImpalaHook(BaseHook):

    def __init__(self, db_name: str, impala_conn_id: str='impala_default', as_dataframe: bool=True):
        """
        Args:
            db_name:
            impala_conn_id:
            as_dataframe:
        """
        self.impala_conn_id = impala_conn_id
        self.client = None
        connection = self.get_connections(self.impala_conn_id)[0]

        self.host = connection.host
        self.port = connection.port
        self.db = db_name
        self.as_dataframe = as_dataframe

        self.logger = logging.getLogger(__name__)
        self.logger.debug(
            '''Connection "{conn}":
            \thost: {host}
            \tport: {port}
            '''.format(
                conn=self.impala_conn_id,
                host=self.host,
                port=self.port
            )
        )

    def get_conn(self):
        """
        Returns an Impala connection.
        """
        if not self.client:
            self.logger.debug(
                'generating impala client for conn_id "{conn}" on '
                '{host}:{port}:{db}'.format(conn=self.impala_conn_id,
                                            host=self.host,
                                            port=self.port,
                                            db=self.db))
            try:
                self.client = connect(host=self.host,
                                      port=self.port,
                                      database=self.db)
            except Exception as general_error:
                raise AirflowException(
                    'Failed to create Impala client, error: {error}'.format(
                        error=str(general_error)
                    )
                )
        return self.client

    def get_data(self, query: str):
        try:
            self.logger.info("trying to establish connection with db")
            conn = self.get_conn()
            cur = conn.cursor()
            self.logger.info("established connection")
            cur.execute()
            if self.as_dataframe:
                df = as_pandas(cur)
            else:
                df = cur.fetchall()
            cur.close()
            conn.close()
        except Exception as e:
            raise AirflowException("Unable to access database: %s" % str(e))
        return df

    def execute_query(self, query: str):
        try:
            self.logger.info("trying to establish connection with db")
            conn = self.get_conn()
            cur = conn.cursor()
            self.logger.info("established connection")
            cur.execute(query)
            self.logger.info("query {} executed".format(query))
            cur.close()
            conn.close()
        except Exception as e:
            raise AirflowException("Unable to access database: %s" % str(e))
