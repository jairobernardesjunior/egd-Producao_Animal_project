''' Baixa arquivos xls de abate da pecuária encontrados no site do IBGE, transforma em arquivos csv e
    ingere estes arquivos no bucket s3 como dados brutos
'''

import os
import datetime
import eventlet
import requests
import urllib.request
import pandas as pd
from fc import fc_download_s3 as ds3
from fc import fc_upload_s3 as ups3

def converte_para_csv(pathArquivo, nome_arquivo):
    # ******************** CONVERTE ARQUIVO PARA .CSV
    arquivoxls = pd.read_excel(pathArquivo)
    pathArquivo_csv = pathArquivo[0: len(pathArquivo) -3] + 'csv'
    arquivoxls.to_csv(pathArquivo_csv, index=False)
    
    if os.path.exists(pathArquivo_csv) == True:
        return pathArquivo_csv
    else:
        print('+++++ erro na conversão do csv: ' + pathArquivo_csv)
        return False

def baixa_arquivo(url, dirAux, nome_arquivo):
    # ******************** BAIXA ARQUIVO DO SITE DO IBGE
    with eventlet.Timeout(None):
        urllib.request.urlretrieve(url, dirAux + '/' + nome_arquivo)

    if os.path.exists(dirAux + '/' + nome_arquivo) == True:
        return dirAux + '/' + nome_arquivo
    else:
        print('+++++ erro ao baixar arquivo(origem): ' + url)
        print('+++++ erro ao baixar arquivo(destino): ' + dirAux + '/' + nome_arquivo)
        return False

def verifica_arquivo(url):
    # ******************** VERIFICA SE ARQUIVO EXISTE NO SITE DO IBGE
    response = requests.get(url, verify=False, timeout=None) 
    return response.status_code

def SalvaUltimaURL(pathArquivo, ultimaURL):
    arquivo = open(pathArquivo,'w')
    arquivo.write(ultimaURL)
    arquivo.close      

def lambda_handler(event, context):
    # ******************** INÍCIO

    dirAux = 'arquivos_baixados_ibge'
    dirPar = 'parametros'
    s3_dados_raw = 'pa-s3-abate-csv-bruto'
    s3_parametros = 'pa-s3-parametros'
    arq_ultimo_baixado = 'ultimo_arq_abate_baixado.txt'
    arq_keys = 'chaves_acesso.txt'
    barra = '/'

    if os.path.exists(dirAux) == False:
        try:
            os.mkdir(dirAux)
        finally:
            fim = 'ok'

    # ******************** LE O ARQUIVO DE CHAVES
    arquivo = open(dirPar + barra + arq_keys,'r')
    access_key = arquivo.readline()
    access_key = access_key[0:len(access_key) -1]
    secret_key = arquivo.readline()
    secret_key = secret_key[0:len(secret_key) -1]
    regiao = arquivo.readline()
    arquivo.close()   

    # ******************** CHAMA A FUNÇÃO DE DOWNLOAD DO BUCKET S3
    retorno = ds3.download_s3(
                s3_parametros, 
                arq_ultimo_baixado, 
                dirPar + barra + arq_ultimo_baixado, 
                access_key, secret_key, regiao)

    if retorno != True:
        print(retorno)
        print('bucket s3 => ' + s3_parametros + ' arquivo => ' + arq_ultimo_baixado + 
              ' ***** não foi baixado')
        exit()

    # ******************** LE O ARQUIVO BAIXADO E OBTEM O ÚLTIMO ARQ_ABATE PROCESSADO
    arquivo = open(dirPar + barra + arq_ultimo_baixado,'r')
    for url in arquivo:
        url = url.rstrip()
    arquivo.close()

    ano= int(url[len(url) -10 : len(url) -6])
    trimestre= int(url[len(url) -6 : len(url) -4])
    ano_corrente = int(datetime.date.today().year)

    # ******************** MONTA O NOME DO ARQUIVO DO ULTIMO TRIMESTRE PARA BAIXAR
    ultimaURL= ''

    if trimestre == 4:
        ano= ano +1    
    
    while (ano_corrente >= ano):
        url2= url[0: len(url) -10] + str('%04d' % ano) + '04.xls'
        nome_arquivo = url2[len(url2) -16: len(url2)]

        if verifica_arquivo(url2) == 200: # verifica se url existe
            resulte = baixa_arquivo(url2, dirAux, nome_arquivo)
            
            if resulte != False:
                resulte = converte_para_csv(resulte, nome_arquivo)
                tri='04'
        else:
            url2= url[0: len(url) -10] + str('%04d' % ano) + '03.xls' 
            nome_arquivo = url2[len(url2) -16: len(url2)] 

            if verifica_arquivo(url2) == 200: # verifica se url existe
                resulte = baixa_arquivo(url2, dirAux, nome_arquivo) 
                if resulte != False:
                    resulte = converte_para_csv(resulte, nome_arquivo)
                    tri='03'
            else:
                url2= url[0: len(url) -10] + str('%04d' % ano) + '02.xls'
                nome_arquivo = url2[len(url2) -16: len(url2)]

                if verifica_arquivo(url2) == 200: # verifica se url existe
                    resulte = baixa_arquivo(url2, dirAux, nome_arquivo)
                    if resulte != False:
                        resulte = converte_para_csv(resulte, nome_arquivo)
                        tri='02'
                else:
                    url2= url[0: len(url) -10] + str('%04d' % ano) + '01.xls' 
                    nome_arquivo = url2[len(url2) -16: len(url2)]

                    if verifica_arquivo(url2) == 200: # verifica se url existe
                        resulte = baixa_arquivo(url2, dirAux, nome_arquivo) 
                        if resulte != False:
                            resulte = converte_para_csv(resulte, nome_arquivo)
                            tri='01'
                    else:
                        print('Ano: ' + str('%04d' % ano) + ' não encontrado') 
                        resulte = False 

        # ******************** CARREGA ARQUIVO RAW CSV NO BUCKET S3
        if resulte != False:
            nome_arquivo = nome_arquivo[0: len(nome_arquivo) -3] + 'csv'
            retorno = ups3.upload_s3(
                    s3_dados_raw, nome_arquivo, resulte, access_key, secret_key, regiao)

            if retorno != True:
                print(retorno)
                print('bucket s3 => ' + s3_dados_raw + ' arquivo => ' + resulte + 
                    ' ***** não foi carregado')
                exit()    
            else:        
                ultimaURL = url2

        ano= ano +1

    # ******************** ALTERA ULTIMO ARQUIVO PROCESSADO E SOBE PARA O BUCKET S3
    if len(ultimaURL) > 0: # grava a posição do último arquivo pmc.xls baixado
        path_arquivo = dirPar + barra + arq_ultimo_baixado
        SalvaUltimaURL(path_arquivo, ultimaURL)
        
        retorno = ups3.upload_s3(
                s3_parametros, arq_ultimo_baixado, path_arquivo, access_key, secret_key, regiao)

        if retorno != True:
            print(retorno)
            print('bucket s3 => ' + s3_parametros + ' arquivo => ' + path_arquivo + 
                  ' --- ' + retorno + ' ***** não foi carregado')
            exit()      

lambda_handler(1, 1)