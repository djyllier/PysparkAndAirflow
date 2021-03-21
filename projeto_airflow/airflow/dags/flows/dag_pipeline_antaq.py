from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.http_operator import SimpleHttpOperator
from tools import (
   extrai_dados_antaq, 
   transforma_dados, 
   salva_dados_db_raw, 
   salva_dados_db_staging
   )


default_args = {
   'owner': 'Djyllier',
   'depends_on_past': False,
   'start_date': datetime(2021, 1, 1),
   'retries': 0,
   }

# Nomeando a DAG
dag = DAG(
   dag_id='Pipeline_ANTAQ',
   description='Pipeline de Dados ANTAQ',
   schedule_interval='0 0 1 * *',
   catchup=False,
   default_args=default_args
   )


dados = ['atracamento', 'tempos_atracamento', 'carga', 'carga_conteinerizada']


# Definindo as tarefas
def get_grupo_dados(dag, previous_task, next_task, dados):

   for dado in dados:

      extracao = SimpleHttpOperator(
        task_id='Extracao_de_dados_{}'.format(dado),
        endpoint='url...',
        method='GET',
        trigger_rule="all_success",
        dag=dag
      )

      email_erro = EmailOperator(
            task_id='Email_Erro_{}'.format(dado),
            to='to@gmail.com',
            subject='Airflow Alert Erro',
            html_content='Erro ao realizar captura de {}'.format(dado),
            dag=dag,
            trigger_rule="all_failed",
            default_args = {
               'email': ['exemplo@gmail.com'],
               'email_on_failure': True,
               'email_on_retry': True,
               'retries': 2,
               'retry_delay': timedelta(minutes=5)
            }
      )

      salvar_base_raw = BranchPythonOperator(
         task_id='Salvar_DB_Raw_{}'.format(dado),
         python_callable= salva_dados_db_raw,
         trigger_rule="all_success",
         dag=dag
      )

      stop_falha = BranchPythonOperator(
         task_id='Stop_erro_extracao_{}'.format(dado),
         python_callable= salva_dados_db_raw,
         trigger_rule="dummy",
         dag=dag
      )

      transformacao = BranchPythonOperator(
         task_id='Transformacao_dados_{}'.format(dado),
         python_callable= transforma_dados,
         trigger_rule="one_success",
         dag=dag
      )

      salvar_base_staging = BranchPythonOperator(
         task_id='Salvar_DB_Staging_{}'.format(dado),
         python_callable= salva_dados_db_staging,
         trigger_rule="all_success",
         dag=dag
      )


#definindo fluxo
      previous_task >> extracao
      extracao >> email_erro
      extracao >> salvar_base_raw
      email_erro >> stop_falha
      stop_falha >> transformacao
      salvar_base_raw >> transformacao
      transformacao >> salvar_base_staging
      salvar_base_staging >> next_task



inicio = DummyOperator(
      task_id='inicio',
      trigger_rule='all_success',
      dag=dag
   )

email_success = EmailOperator(
      task_id='Email_sucesso',
      to='to@gmail.com',
      subject='Airflow Alert Success',
      html_content=""" <h3>Email Test Success</h3> """,
      dag=dag,
      trigger_rule="all_success",
      default_args = {
         'email': ['some_email@gmail.com'],
         'email_on_failure': True,
         'email_on_retry': True,
         'retries': 1,
         'retry_delay': timedelta(minutes=5)
      }
)
fim = DummyOperator(
      task_id='fim',
      trigger_rule='all_success',
      dag=dag
      
   )


# Definindo fluxo
 
grupo_dados = get_grupo_dados(dag, inicio, email_success, dados)

email_success >> fim
