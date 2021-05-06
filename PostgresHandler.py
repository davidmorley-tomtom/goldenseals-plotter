import pandas as pd

from azure.keyvault.secrets import SecretClient
from azure.identity import AzureCliCredential
from psycopg2.pool import ThreadedConnectionPool


class PostgresHandler:
	def __init__(self, conn_dict: dict, opts_dict: dict):
		self._conn_dict = conn_dict
		self._opts_dict = opts_dict
		self._pool = ThreadedConnectionPool(minconn=10,
											maxconn=20,
											user=self._conn_dict['user'],
											password=self._password(),
											host=self._conn_dict['hostname'],
											port=self._conn_dict['port'],
											database=self._conn_dict['database'])
		self.execute_query(self.get_hex2dec_function_sql())

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

	def get_dataframe(self) -> pd.DataFrame:
		run_id = self._opts_dict['headers_correlationid']
		if run_id == '':
			# Get the most recent id
			sql = """
				select "headers_correlationId" 
				from public."geometry-restriction-classifications"
				order by "headers_eventTime" desc limit 1;
			"""
			run_id = self.execute_select_query(sql)[0][0]
		return self.execute_query_to_pandas(self.get_validation_table_sql(run_id))

	def get_validation_table_sql(self, hc_id: str) -> str:
		return """
			select moderated.osm_id, moderated.obsv, modelled.pred, modelled.body_confidence
			from 
				(select hex_to_int(substring("CLASSIFIEDFEATURE_FEATURE_FEATUREID", 29, 32)) as osm_id,
					case
						when trace_behavior_backward = 'OPEN' and trace_behavior_forward = 'OPEN' then 'OPEN_BOTH'
						when trace_behavior_backward = 'OPEN' and trace_behavior_forward = 'CLOSED' then 'OPEN_NEG' 
						when trace_behavior_backward = 'CLOSED' and trace_behavior_forward = 'OPEN' then 'OPEN_POS'
						when trace_behavior_backward = 'CLOSED' and trace_behavior_forward = 'CLOSED' then 'CLOSED'
						else 'UNKNOWN' end as obsv
				from {0}
				where country_code = '{1}') moderated
			left join 
				(select distinct a.body_confidence, a.pred, b."BODY_EDGEID"::int as osm_id from
					(select "headers_causationId", body_classification as pred, body_confidence
					from public."geometry-restriction-classifications"
					where "headers_correlationId" = '{2}'
					and body_classification = any(array['OPEN_POS', 'OPEN_NEG', 'OPEN_BOTH', 'CLOSED'])) a
				left join 
					public."geometry-restriction-features-db" b
				on a."headers_causationId" = b."HEADERS_ID") modelled
			on moderated.osm_id = modelled.osm_id;
		""".format(self._opts_dict['validation_table'], self._opts_dict['country_code'], hc_id)

	@staticmethod
	def get_hex2dec_function_sql() -> str:
		return """
			create or replace function hex_to_int(hexval varchar) returns integer as $$
			declare
				result  int;
			begin
				execute 'select x' || quote_literal(hexval) || '::int' into result;
				return result;
			end;
			$$ language plpgsql immutable strict;
		"""

	def _password(self) -> str:
		return self._get_secret_from_key_vault(self._conn_dict['keyvault'], self._conn_dict['password_secret'])

	@staticmethod
	def _get_secret_from_key_vault(key_vault_name: str, secret_name: str) -> str:
		KVUri = f"https://{key_vault_name}.vault.azure.net"
		client = SecretClient(vault_url=KVUri, credential=AzureCliCredential())
		return client.get_secret(secret_name).value

	def destroy(self):
		self._pool.closeall()
