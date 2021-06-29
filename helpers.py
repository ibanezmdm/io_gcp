
def params(*args):

	params = {
		'gcp_table': None, 
		# 'path_file': None,
		'file_csv': None,
		'query': None
	}

	flags = {
		# 'path_file': ['p', 'c', 'path', 'path_file', 'carpeta'],
		'file_csv': ['f', 'n', 'a', 'file', 'file_csv', 'nombre', 'archivo', 'nombre_archivo'],
		'query': ['q', 'query', 'consulta'],
		'gcp_table': ['g','t', 'gt', 'gcp_table']
	}
	
	for flag in flags.keys():
		compara = list(set(args[0].keys()).intersection(flags[flag]))
		if len(compara) > 1:
			raise RuntimeError ('Se esta definiendo mas de una vez el parametro {}'.format(flag))
		elif len(compara) == 1:
			params[flag] = args[0][compara[0]]
			# print (args[0][compara[0]])

	return params

if __name__ == '__main__':
	dict = {'q': 'query', 'p': 'path'}
	result = params(dict)
	print (result)