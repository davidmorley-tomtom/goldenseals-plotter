import pandas as pd
from azure.keyvault.secrets import SecretClient
from azure.identity import AzureCliCredential
from psycopg2.pool import ThreadedConnectionPool


class PostgresHandler:
	def __init__(self, conn_dict: dict):
		self._conn_dict = conn_dict
		self._pool = ThreadedConnectionPool(minconn=10,
											maxconn=20,
											user=self._conn_dict['user'],
											password=self._password(),
											host=self._conn_dict['hostname'],
											port=self._conn_dict['port'],
											database=self._conn_dict['database'])

	def execute_query_to_pandas(self, query) -> pd.DataFrame:
		connection = self._pool.getconn()
		try:
			with connection:
				return pd.read_sql_query(query, connection)
		finally:
			self._pool.putconn(connection)

	def execute_query(self, query):
		connection = self._pool.getconn()
		try:
			with connection:
				with connection.cursor() as cursor:
					cursor.execute(query)
					connection.commit()
		finally:
			self._pool.putconn(connection)

	def execute_select_query(self, query: str) -> list:
		connection = self._pool.getconn()
		try:
			with connection:
				with connection.cursor() as cursor:
					cursor.execute(query)
					return cursor.fetchall()
		finally:
			self._pool.putconn(connection)

	def _password(self) -> str:
		return self._get_secret_from_key_vault(self._conn_dict['keyvault'], self._conn_dict['password_secret'])

	@staticmethod
	def _get_secret_from_key_vault(key_vault_name: str, secret_name: str) -> str:
		KVUri = f"https://{key_vault_name}.vault.azure.net"
		client = SecretClient(vault_url=KVUri, credential=AzureCliCredential())
		return client.get_secret(secret_name).value

	def destroy(self):
		self._pool.closeall()
