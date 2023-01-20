''' Le arquivos csv com dados brutos, faz a transformação dos dados e ingere em bucket s3,
    em banco redshift e em banco dynamoDB
'''

import os
import datetime
import pandas as pd
import redshift_connector as rsc
import pandas_redshift as pr
from fc import fc_download_s3 as ds3
from fc import fc_upload_s3 as ups3
from fc import fc_monta_dados_abate as mga

def grava_sobes3_arquivo_json_lapidado(
        dirAux, nome_arquivo, df, s3_dados_processed, access_key, secret_key, regiao):
    nome_arquivocsv = nome_arquivo
    nome_arquivo2 = nome_arquivo[0: len(nome_arquivo) -3]

    nome_arquivo = nome_arquivo[0: len(nome_arquivo) -3] + 'json'
    pathJson = dirAux + '/' + nome_arquivo

    df.to_string(dirAux + '/' + nome_arquivo2 + 'txt')
    df.to_json(dirAux + '/' + nome_arquivo2 + 'json')

    # ******************** CARREGA ARQUIVO json NO BUCKET S3
    retorno = ups3.upload_s3(
            s3_dados_processed, nome_arquivo, pathJson, access_key, secret_key, regiao)

    if retorno != True:
        print(retorno)
        print('bucket s3 => ' + s3_dados_processed + ' arquivo => ' + nome_arquivo + 
                ' --- ' + retorno + ' ***** não foi carregado')
        exit()     

    return nome_arquivocsv    

def le_arqcsv_grava_dados_lapidados(
        dirAux, nome_arquivo, ano, tri, s3_dados_processed, access_key, secret_key, regiao,
        host_i, database_i, user_i, password_i, porta_i):

    # ******************** LE O ARQUIVO CSV A SER LAPIDADO E GRAVA EM BASE DE DADOS
    tabela= pd.read_csv(dirAux + '/' + nome_arquivo)
    retorno_df= mga.monta_dados_abate(tabela, ano, tri)

    nome_arquivocsv =  grava_sobes3_arquivo_json_lapidado(
                            dirAux, nome_arquivo, retorno_df, s3_dados_processed, 
                            access_key, secret_key, regiao)

    # ******************** INCLUI DF NA TABELA TB_ABATE DO REDSHIFT
    pr.connect_to_redshift(dbname = database_i,
                            host = host_i,
                            port = porta_i,
                            user = user_i,
                            password = password_i)

    pr.connect_to_s3(aws_access_key_id = access_key,
                        aws_secret_access_key = secret_key,
                        bucket = s3_dados_processed,
                        subdirectory = 'df')

    # Write the DataFrame to S3 and then to redshift
    pr.pandas_to_redshift(data_frame = retorno_df,
                          redshift_table_name = 'tb_abate')

    # ******************** INCLUI DF NA TABELA TB_ABATE DO DYNAMODB


    
    return nome_arquivocsv

def SalvaUltimoARQ(pathArquivo, ultimoARQ):
    arquivo = open(pathArquivo,'w')
    arquivo.write(ultimoARQ)
    arquivo.close                 

def lambda_handler(event, context):
    # ******************** INÍCIO
    dirAux = 'arquivos_baixados_ibge'
    dirPar = 'parametros'
    s3_dados_raw = 'pa-s3-abate-csv-bruto'
    s3_dados_processed = 'pa-s3-abate-json-transformados'
    s3_parametros = 'pa-s3-parametros'
    arq_ultimo_lapidado = 'ultimo_arq_csv_lapidado.txt'
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
    regiao = regiao[0:len(regiao) -1]

    host_i = arquivo.readline()
    host_i = host_i[0:len(host_i) -1]

    database_i = arquivo.readline()
    database_i = database_i[0:len(database_i) -1]

    user_i = arquivo.readline()
    user_i = user_i[0:len(user_i) -1]

    password_i = arquivo.readline()  
    password_i = password_i[0:len(password_i) -1]    

    porta_i = arquivo.readline()     

    arquivo.close()   

    # ******************** CHAMA A FUNÇÃO DE DOWNLOAD DO BUCKET S3 - BAIXA .CSV BRUTO
    retorno = ds3.download_s3(
                s3_parametros, 
                arq_ultimo_lapidado, 
                dirPar + barra + arq_ultimo_lapidado, 
                access_key, secret_key, regiao)

    if retorno != True:
        print(retorno)
        print('bucket s3 => ' + s3_parametros + ' arquivo => ' + arq_ultimo_lapidado + 
              ' ***** não foi baixado')
        exit()

    # ******************** LE O ARQUIVO BAIXADO E OBTEM DADOS DO ARQ CSV BRUTO
    arquivo = open(dirPar + barra + arq_ultimo_lapidado,'r')
    for arqCSV in arquivo:
        arqCSV = arqCSV.rstrip()
    arquivo.close()

    ano= int(arqCSV[len(arqCSV) -10 : len(arqCSV) -6])
    trimestre= int(arqCSV[len(arqCSV) -6 : len(arqCSV) -4])
    ano_corrente = int(datetime.date.today().year)

    # ******************** CONECTA O REDSHIFT APAGA TRIMESTRE MENOR QUE 4
    conn = rsc.connect(
        host=host_i,
        database=database_i,
        user=user_i,
        password=password_i)

    with conn.cursor() as sql_cursor:
        sql_cursor.execute('delete from tb_abate where trimestre<4')

    sql_cursor.close
    conn.commit
    conn.close

    # ******************** CONECTA O dynamoDB
    

    # ******************** MONTA O NOME DO ARQUIVO CSV DO ULTIMO TRIMESTRE
    if trimestre == 4:
        ano= ano +1    
    
    ultimoARQ= ''
    while (ano_corrente >= ano):
        nome_arquivo= arqCSV[0: len(arqCSV) -10] + str('%04d' % ano) + '04.csv'
        retorno = ds3.download_s3(
                    s3_dados_raw,
                    nome_arquivo, 
                    dirAux + barra + nome_arquivo, 
                    access_key, secret_key, regiao)
        if retorno == True:
            ultimoARQ = le_arqcsv_grava_dados_lapidados(
                dirAux, nome_arquivo, ano, '04', 
                s3_dados_processed, access_key, secret_key, regiao,
                host_i, database_i, user_i, password_i, porta_i)   
        else:

            nome_arquivo= arqCSV[0: len(arqCSV) -10] + str('%04d' % ano) + '03.csv'
            retorno = ds3.download_s3(
                        s3_dados_raw,
                        nome_arquivo, 
                        dirAux + barra + nome_arquivo, 
                        access_key, secret_key, regiao)
            if retorno == True:
                ultimoARQ = le_arqcsv_grava_dados_lapidados(
                    dirAux, nome_arquivo, ano, '03',
                    s3_dados_processed, access_key, secret_key, regiao,
                    host_i, database_i, user_i, password_i, porta_i)              
            else:

                nome_arquivo= arqCSV[0: len(arqCSV) -10] + str('%04d' % ano) + '02.csv'
                retorno = ds3.download_s3(
                            s3_dados_raw,
                            nome_arquivo, 
                            dirAux + barra + nome_arquivo, 
                            access_key, secret_key, regiao)
                if retorno == True:
                    ultimoARQ = le_arqcsv_grava_dados_lapidados(
                        dirAux, nome_arquivo, ano, '02',
                        s3_dados_processed, access_key, secret_key, regiao,
                        host_i, database_i, user_i, password_i, porta_i)                 
                else:

                    nome_arquivo= arqCSV[0: len(arqCSV) -10] + str('%04d' % ano) + '01.csv'
                    retorno = ds3.download_s3(
                                s3_dados_raw,
                                nome_arquivo, 
                                dirAux + barra + nome_arquivo, 
                                access_key, secret_key, regiao)
                    if retorno == True:
                        ultimoARQ = le_arqcsv_grava_dados_lapidados(
                            dirAux, nome_arquivo, ano, '01',
                            s3_dados_processed, access_key, secret_key, regiao,
                            host_i, database_i, user_i, password_i, porta_i)                  
                    else:                                    
                        print('***** arquivo: ' + dirAux + barra + nome_arquivo + ' não encontrado') 
                        resulte = False 

        ano= ano +1

    # ******************** ALTERA ULTIMO ARQUIVO PROCESSADO E SOBE PARA O BUCKET S3
    if len(ultimoARQ) > 0: # grava a posição do último arquivo .csv processado
        path_arquivo = dirPar + barra + arq_ultimo_lapidado
        SalvaUltimoARQ(path_arquivo, ultimoARQ)
        
        retorno = ups3.upload_s3(
                s3_parametros, arq_ultimo_lapidado, path_arquivo, access_key, secret_key, regiao)

        if retorno != True:
            print(retorno)
            print('bucket s3 => ' + s3_parametros + ' arquivo => ' + path_arquivo + 
                  ' --- ' + retorno + ' ***** não foi carregado')
            exit()      

lambda_handler(1, 1)