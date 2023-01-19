''' recebe dados de abate, monta e grava arq
'''

import pandas as pd
import strip

def monta_dados_abate(tabela, ano_i, tri):

    mes= []
    ano= []
    trimestre= []
    quantidade_bovinos= []
    quantidade_suinos=[]   
    quantidade_frangos=[]          

    idx= ((int(tri) -1) *4) +7
    valor = str(tabela.iloc[idx, 2])
    
    if len(valor) > 0:
        i= 1
        while i<4:
            mes.append(tabela.iloc[idx, 0])
            ano.append(ano_i)
            trimestre.append(str(int(tri)))
            quantidade_bovinos.append(tabela.iloc[idx, 2])        
            quantidade_suinos.append(tabela.iloc[idx, 5])    
            quantidade_frangos.append(tabela.iloc[idx, 8])  

            i= i+1
            idx= idx+1

        if i>1:
            df=pd.DataFrame({
                    "mes":mes,
                    "ano":ano,
                    "trimestre":trimestre,
                    "quantidade_bovinos":quantidade_bovinos,
                    "quantidade_suinos":quantidade_suinos,
                    "quantidade_frangos":quantidade_frangos,
                    })

            return df 
        else:
            print('erro ao gerar o data frame no fc_monta_dados_abate.py')
            exit() 