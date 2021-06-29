import sys
import helpers
from downloader.downloader import Downloader as dw

def main():

	metodo = sys.argv[1]
	csv_path = sys.argv[2]

	dict = {}
	for arg in sys.argv[3:]:
		dict[arg.split('=')[0]] = arg.split("=")[1]


	if metodo in ['d', 'downloader', 'descarga']:
		params = helpers.params(dict)
		params['csv_path'] = csv_path
		print (params)
		proceso = dw(params)
		print(proceso)
		proceso.exect()
	elif metodo not in ['u', 'c', 'uploader', 'carga']:
		# TODO: falta implementar modulod de carga
		pass
	else:
		raise RuntimeError('No se esta definiendo metodo de carga o descarga')

	# print (dict)
	# print (sys.argv[1:])

	# raise RuntimeError("se esta declarando dos veces la misma variable")


if __name__ == '__main__':
	main()