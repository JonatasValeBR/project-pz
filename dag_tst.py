from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging


# 1. Definimos a funÃ§Ã£o Python que serÃ¡ executada pela Task
def minha_funcao_de_teste(**kwargs):
    # O comando 'print' comum vai aparecer nos logs da task
    print("OlÃ¡! Este Ã© um log gerado por um comando 'print' simples.")

    # Ã‰ uma boa prÃ¡tica usar a biblioteca 'logging' nativa para logs mais profissionais
    logger = logging.getLogger(__name__)
    logger.info("Este Ã© um log gerado pelo mÃ³dulo 'logging' com nÃ­vel INFO.")

    # Como bÃ´nus, pegamos informaÃ§Ãµes do contexto da DAG (data de execuÃ§Ã£o, etc)
    data_execucao = kwargs.get('ds')
    logger.info(f"Esta DAG estÃ¡ rodando com a data de referÃªncia: {data_execucao}")

    return "Teste finalizado com sucesso!"


# 2. Estrutura da DAG
with DAG(
        dag_id='dag_labcgti_hello_0.01',  # Mudei levemente o ID para refletir o teste
        start_date=datetime(2026, 4, 14),
        schedule=None,  # ExecuÃ§Ã£o manual
        catchup=False,
        tags=['cgti'],
) as dag:
    # Task de inÃ­cio (opcional, mas bom para organizaÃ§Ã£o visual)
    inicio = EmptyOperator(task_id='inicio')

    # 3. Criamos a Task que vai chamar a nossa funÃ§Ã£o Python
    task_python = PythonOperator(
        task_id='meu_primeiro_codigo_python',
        python_callable=minha_funcao_de_teste,
    )

    # A sua task original de sucesso
    sucesso_total = EmptyOperator(task_id='sucesso_total')

    # 4. Definimos a ordem de execuÃ§Ã£o (dependÃªncias)
    inicio >> task_python >> sucesso_total