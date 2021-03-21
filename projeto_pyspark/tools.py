from requests import Session
import zipfile 
import os
import io
import shutil


def download_antaq(dado_download, ano):


    s = Session()

    response = s.get(dado_download['url_download'].format(ano))

    file_zip_path = os.path.join(os.path.abspath('.'), 'arquivos_download', dado_download['nome_pasta'])

    files = zipfile.ZipFile(io.BytesIO(response.content))

    files.extractall(path=file_zip_path)


def concatena_dataframe(spark, anos, dados_arquivo):


    folder_path = os.path.join(os.path.abspath('.'), 'arquivos_download', dados_arquivo['nome_pasta'])

    file_path = os.path.join(folder_path, anos[0] + dados_arquivo['nome_arquivo'] + '.txt')
    df_full = spark.read.option('delimiter', ';').option('inferSchema', True).option('header', True).csv(file_path)

    for index in range(1, len(anos)):
        
        file_path = os.path.join(folder_path, anos[index] + dados_arquivo['nome_arquivo']  + '.txt')
        df_full = df_full.union(spark.read.option('delimiter', ';').option('inferSchema', True).option('header', True).csv(file_path))


    return df_full


def salva_arquivo(df, fato):


    pasta_resultado = os.path.join(os.path.abspath('.'), 'resultado_tab_fato')

    if not os.path.exists(pasta_resultado):

        os.mkdir(pasta_resultado)

    df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option('sep',';').save("csv_tmp")

    csv_tmp = os.path.join(os.path.abspath('.'), 'csv_tmp')
    arquivos = os.listdir(csv_tmp)

    for arquivo in arquivos:
        if '.csv' in arquivo and not '.crc' in arquivo:

            caminhoAtual = os.path.join(csv_tmp, arquivo)
            caminhoNovo = os.path.join(pasta_resultado, fato + '.csv') 

            shutil.move(caminhoAtual, caminhoNovo)

    shutil.rmtree(csv_tmp)
