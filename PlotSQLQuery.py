class PlotSQLQuery:

	def __init__(self, opts_dict: dict):
		self._opts_dict = opts_dict

	def get_most_recent_run(self) -> str:
		return """
			select "headers_correlationId" 
			from public."geometry-restriction-classifications"
			order by "headers_eventTime" desc limit 1;
		"""

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

	def get_hex2dec_function_sql(self) -> str:
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
