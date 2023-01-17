def guardaUltimaURL(pathArquivo, URL):
    arquivo = open(pathArquivo,'w')
    arquivo.write(URL)
    arquivo.close  