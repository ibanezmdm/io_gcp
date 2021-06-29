from google.cloud import bigquery
import datetime
import requests
import yaml
import os


class Process:
	def __init__(self):
		# NICIALIZANDO EL CLIENTE DE FORMA EXPLICITA                
		# json_key = os.path.join(os.path.dirname(__file__),'btot-pe-prd-businessinsights-1f85530c7945.json')
		self.bq_client = bigquery.Client.from_service_account_json('tot-bi-cl-logistica-dev-dbebbf94a4f0.json')
		self.file_csv = 'csv_instock_opt_base_gcp_' + datetime.datetime.now().strftime("%Y%m%d")+'.csv'
		self.path_file = "C:\gcp\instock"

    
	def _execute_query(self, BigQuery):
		print(requests.certs.where())
		stream = open('config.yml', 'r')
		settings = yaml.load(stream, yaml.SafeLoader)
		os.environ["HTTP_PROXY"] = '{}'.format(settings['HTTP_PROXY'])
		os.environ["HTTPS_PROXY"] = '{}'.format(settings['HTTPS_PROXY'])
		
		sql = """ 
			SELECT *
			FROM `tot-bi-cl-logistica-dev.acc_tot_corp_kpis_logisticos.instock_opt_base_gcp`
		"""
		print('Intentando Query...')
		# Execute Query
		query_job = BigQuery.query(sql)
		#query_job.result()  # Waits for the query to finish
		print('Query completo.')
		df_BQ = BigQuery.query(sql).to_dataframe()
		#metadata DATAFRAME
		#df_BQ.info()
		#df_BQ.head(5)
		print('Cargado en DataFrame')
		df_BQ.to_csv(os.path.join(self.path_file, self.file_csv), sep=',', index = False, encoding = 'utf-8')
		print('Se cargo archivo .CSV')
		return print('Guardo correctamente')


	def _main(self):
		BigQuery = self.bq_client
		try:
			self._execute_query(BigQuery)
		except Exception as error:
			print(error)



if __name__ == "__main__":
	print("PROCESO DE EXTRACCION\n")
	proceso = Process()
	proceso._main()