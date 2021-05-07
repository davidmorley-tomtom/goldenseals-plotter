from PlotsGoldensealsKR1 import PlotsGoldensealsKR1

# Moderation DB connection details
CONNECTION = {
	'keyvault': 'cfv-development',
	'password_secret': 'postgres-dba-password',
	'hostname': 'psql-amf-cfv-dev.postgres.database.azure.com',
	'port': '5432',
	'database': 'cfv-connect-db',
	'user': 'dba_admin@psql-amf-cfv-dev'
}

# Moderation location and which predictions to use
# Leave 'headers_correlationid' blank to use most recent
OPTIONS = {
	'validation_table': 'public."cfv-validate-response-exploded-validation_march2021_NLD_CA"',
	'country_code': 'NLD',
	'headers_correlationid': '54144edb-924c-4073-bb79-39acd1ffec48',
	'output_dir': 'C:/temp2',
	'threshold': 0.9
}


if __name__ == '__main__':
	plotter = PlotsGoldensealsKR1(CONNECTION, OPTIONS)
	plotter.plot_confusion_matrix(save=False)
	plotter.plot_precision_recall(save=False)
	print("## Finished Plotting ##")
