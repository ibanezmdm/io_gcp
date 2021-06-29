from google.cloud import bigquery
import datetime
import requests
import yaml
import os


class Downloader:
	""" Clase para descargar data desde gcp a un archivo csv.

	Attributes:
		bq_client (`bigquery.Client`): Almacena cuenta de servicio para conectarse a gcp.
		gcp_table (:obj:`string`, optional): Nombre de tabla en gcp bigquery.
		path_file (:obj:`string`, optional): Directorio donde se guardara el archivo csv.
		file_csv (:obj:`string`, optional): Nombre con el cual se guardara el archivo csv.
		query (:obj:`string`, optional): Query personalizada a ejecutar en gcp.

	Todo:
		* implementar creacion automatica de format file.
		* implementar creacion automatica de job de carga en sql.

	"""
	
	def __init__(self, params):
		""" Metodo de __init__ para clase Downloader

		Existen dos metodos de poder ejecutar la descarga de archivos desde gcp:

		Declarando los parametros `csv_path` y `gcp_table`: Esta forma se utiliza si tiene una tabla lista con la informacion, se tomara el nombre de esta para general la query de forma automatica.

		Declarando los parametros `csv_path`, `query` y `file_csv`: Esta forma se utiliza cuando necesitamos ejecutar una query antes de poder descargar la informacion (no todas las consultas funcionan), es por esto que tambien tenemos que declarar el nombre del archivo a guardar.

		Note: 
			Si se declara `gcp_table`, dejara sin efecto la variable `query`.
			Si no se declara `file_csv` el programa tomara como nombre por defecto el valor declarado en `gcp_table`.

		Args:
			gcp_table (:obj:`string`, optional): Nombre de tabla en gcp bigquery.
			path_file (:obj:`string`, optional): Directorio donde se guardara el archivo csv.
			file_csv (:obj:`string`, optional): Nombre con el cual se guardara el archivo csv.
			query (:obj:`string`, optional): Query personalizada a ejecutar en gcp.

		"""
		self.settings = self.__load_settings()
		self.csv_ext = '_' + datetime.datetime.now().strftime("%Y%m%d") + '.csv'
		self.bq_client = bigquery.Client.from_service_account_json('{}'.format(self.settings['SERVICE_ACCOUNT']))
		self.gcp_table = params['gcp_table']
		self.path_file = 'C:\gcp\{}'.format(params['csv_path'])
		self.file_csv = (self.gcp_table if self.gcp_table is not None else params['file_csv']) + self.csv_ext
		self.query = 'SELECT * FROM `tot-bi-cl-logistica-dev.acc_tot_corp_kpis_logisticos.{}`'.format(params['gcp_table']) if params['query'] is None else params['query']


	def __load_settings(self):
		stream = open('config.yml', 'r')
		settings = yaml.load(stream, yaml.SafeLoader)
		stream.close()
		return settings


	def __download_csv(self):
		print(requests.certs.where())
		os.environ["HTTP_PROXY"] = '{}'.format(self.settings['HTTP_PROXY']) 
		os.environ["HTTPS_PROXY"] = '{}'.format(self.settings['HTTPS_PROXY'])
		print('Intentando Query...')
		df_BQ = self.bq_client.query(self.query).to_dataframe()
		print('cargando archivo csv')
		print (self.path_file)
		os.makedirs(self.path_file, exist_ok=True)
		df_BQ.to_csv(os.path.join(self.path_file, self.file_csv), sep=',', index = False, encoding = 'utf-8')
		print('carga archivo csv completa')
		return print('Guardo correctamente')


	def exect(self):
		try:
			self.__download_csv()
		except Exception as error:
			print(error)

	def __str__(self) -> str:
		return self.file_csv


if __name__ == "__main__":
	print("PROCESO DE EXTRACCION\n")
	proceso = Downloader('instock', gcp_table='instock_opt_base_gcp', file_csv='instock_test')
	proceso.exect()