from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from tools import download_antaq, concatena_dataframe, salva_arquivo


spark = SparkSession.builder.master('local[*]').getOrCreate()

dados_anos = ['2018', '2019', '2020']
dados_download = {
    'atracacao': {
        'nome_pasta': 'atracacao',
        'nome_arquivo': 'Atracacao', 
        'url_download': 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}Atracacao.zip'
    }, 
    'tempos_atracacao': {
        'nome_pasta': 'tempos_atracacao',
        'nome_arquivo': 'TemposAtracacao', 
        'url_download': 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}TemposAtracacao.zip'
    },
    'carga': {
        'nome_pasta': 'carga',
        'nome_arquivo': 'Carga', 
        'url_download': 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}Carga.zip'
    },
    'carga_conteinerizada': {
        'nome_pasta': 'carga_conteinerizada',
        'nome_arquivo': 'Carga_Conteinerizada', 
        'url_download': 'http://web.antaq.gov.br/Sistemas/ArquivosAnuario/Arquivos/{}CargaConteinerizada.zip'
    }
}

#download arquivos brutos
for dado in dados_download.values():
    for ano in dados_anos:

        download_antaq(dado, ano)


#Atracacao dataframe completo
df_atracacao_full = concatena_dataframe(spark, dados_anos, dados_download['atracacao'])


#Atracacao dataframe filtro ceara
df_atracacao_ce = df_atracacao_full.filter(df_atracacao_full.UF == 'Ceará')


df_atracacao_full = None


#Atracacao dataframe pré join carga
df_atracacao_id_ce = df_atracacao_ce.select(col('IDAtracacao'), col('Ano'), col('Mes'), col('Porto Atracação'), col('SGUF'))
df_atracacao_id_ce = df_atracacao_id_ce.withColumnRenamed('Ano', 'Ano da data de início da operação da atracação')
df_atracacao_id_ce = df_atracacao_id_ce.withColumnRenamed('Mes', 'Mês da data de início da operação da atracação')


#Tempo Atracacao dataframe completo
df_tempos_atracacao_full = concatena_dataframe(spark, dados_anos, dados_download['tempos_atracacao'])


#join Atracacao e Tempo Atracacao
df_atracacao_ce = df_atracacao_ce.join(df_tempos_atracacao_full,
    df_tempos_atracacao_full.IDAtracacao == df_atracacao_ce.IDAtracacao,
    'left').drop(df_tempos_atracacao_full.IDAtracacao)


df_atracacao_ce = df_atracacao_ce.withColumnRenamed('Ano', 'Ano da data de início da operação')
df_atracacao_ce = df_atracacao_ce.withColumnRenamed('Mes', 'Mês da data de início da operação')


#salva arquivo csv de atracacao
salva_arquivo(df_atracacao_ce, 'atracacao_fato')


#Carga dataframe completo
df_carga_full = concatena_dataframe(spark, dados_anos, dados_download['carga'])

df_carga_ce = df_atracacao_id_ce.join(df_carga_full,
    df_carga_full.IDAtracacao ==  df_atracacao_id_ce.IDAtracacao,
    'left').drop(df_carga_full.IDAtracacao)

df_carga_full = None

#Carga conteinerizada dataframe completo
df_carga_conteinerizada = concatena_dataframe(spark, dados_anos, dados_download['carga_conteinerizada'])


#join carga e carga conteinerizada
df_carga_ce = df_carga_ce.join(df_carga_conteinerizada, [df_carga_conteinerizada.IDCarga ==  df_carga_ce.IDCarga, 
    df_carga_ce['Carga Geral Acondicionamento']=='Conteinerizada'], 'left').drop(df_carga_conteinerizada.IDCarga)


#inclusao codigo produto conteiner
df_carga_ce = df_carga_ce.withColumn('CDMercadoria', 
    when(col('CDMercadoriaConteinerizada').isNotNull(), 
    df_carga_ce['CDMercadoriaConteinerizada']).otherwise(col('CDMercadoria'))).drop(df_carga_ce.CDMercadoriaConteinerizada)


#inclusao peso liquido
df_carga_ce = df_carga_ce.withColumn('VLPesoCargaConteinerizada', 
    when(col('Carga Geral Acondicionamento')=='Conteinerizada', 
    df_carga_ce['VLPesoCargaConteinerizada']).otherwise(col('VLPesoCargaBruta')))

df_carga_ce = df_carga_ce.withColumnRenamed('VLPesoCargaConteinerizada', 'Peso líquido da carga')


#salva arquivo csv de atracacao
salva_arquivo(df_carga_ce, 'carga_fato')

