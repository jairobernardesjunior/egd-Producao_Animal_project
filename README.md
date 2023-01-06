# Producao_Animal
O projeto Produção Animal baixa os arquivos abate_XXXXXX.xls de Pesquisa Trimestral do Abate de Animais, 
que são disponibilizados na página de download da agricultura e pecuária do IBGE 
(https://www.ibge.gov.br/estatisticas/economicas/agricultura-e-pecuaria/9203-pesquisas-trimestrais-do-abate-de-animais.html?=&t=downloads), 
transforma e grava os dados de abate e de carcaça na tab_Animais_Abatidos_eCarcacas em arquivo parquet, 
no banco de dados estruturado lake house aws redshift e no banco de dados noSQL dynamoDB, 
utilizando tecnologia de contêiner Docker para o workload.
