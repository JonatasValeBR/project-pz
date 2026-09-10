from datetime import datetime
from airflow.decorators import dag, task
from kubernetes.client import models as k8s  # ImportaÃ§Ã£o necessÃ¡ria para os objetos K8s

# V7 base
# ConfiguraÃ§Ãµes padrÃ£o da DAG
default_args = {
    'owner': 'seu_nome',
    'start_date': datetime(2026, 1, 1),
}


@dag(
    dag_id='teste_imagem_base_scraping_playwright_V4',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=['teste', 'scraping', 'kubernetes', 'hoerlle'],
)
def teste_imagem_base_scraping_hoerlle_dag():
    # VariÃ¡veis de ConfiguraÃ§Ã£o do Kubernetes
    toleracoes_svc = [
        k8s.V1Toleration(
            key="env",
            operator="Equal",
            value="svc",
            effect="NoSchedule"  # Garanta que bate com o efeito do seu Taint (NoSchedule ou NoExecute)
        )
    ]

    afinidade_svc = k8s.V1Affinity(
        node_affinity=k8s.V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=k8s.V1NodeSelector(
                node_selector_terms=[
                    k8s.V1NodeSelectorTerm(
                        match_expressions=[
                            k8s.V1NodeSelectorRequirement(
                                key="env",
                                operator="In",
                                values=["svc"]
                            )
                        ]
                    )
                ]
            )
        )
    )

    # Esta tarefa serÃ¡ executada de forma isolada dentro do container Kubernetes
    @task.kubernetes(
        image="southamerica-east1-docker.pkg.dev/pgfn-cgti/repo-airflow-custom/airflow-scrapping-base:v6",
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        namespace="airflow",  # Adicionado: Namespace
        tolerations=toleracoes_svc,  # Adicionado: Tolerations
        affinity=afinidade_svc  # Adicionado: Node Affinity
    )
    def checar_componentes_e_ambiente():
        import os
        import sys
        print("====== 1. VERIFICANDO AMBIENTE ======")
        print(f"VersÃ£o do Python rodando no Container: {sys.version}")
        print(f"DiretÃ³rio atual de execuÃ§Ã£o: {os.getcwd()}")

        print("\n====== 2. TESTANDO IMPORTAÃ‡Ã•ES DE TERCEIROS ======")
        try:
            import pandas as pd
            print(f"[OK] Pandas importado com sucesso (VersÃ£o: {pd.__version__})")

            import pyarrow
            print("[OK] PyArrow importado com sucesso")

            from google.cloud import bigquery
            print("[OK] Google Cloud BigQuery importado com sucesso")

            from bs4 import BeautifulSoup
            print("[OK] BeautifulSoup4 importada com sucesso")

            import dotenv
            print("[OK] Python-dotenv importado com sucesso")

            import openpyxl
            print(f"[OK] Openpyxl importado com sucesso (VersÃ£o: {openpyxl.__version__})")

            from tenacity import retry
            print("[OK] Tenacity (Retry) importada com sucesso")

        except ImportError as e:
            print(f"[ERRO] Falha ao importar biblioteca Python: {e}")
            raise e

        print("\n====== 3. TESTE DE FOGO: OPERANDO PLAYWRIGHT (ASYNC) ======")
        import asyncio
        from playwright.async_api import async_playwright

        async def rodar_navegador_teste():
            print("Iniciando instÃ¢ncia do Chromium via Playwright...")
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-dev-shm-usage"]
                )

                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                )

                page = await context.new_page()

                # --- CORREÃ‡ÃƒO DO IMPORT E APLICAÃ‡ÃƒO DO STEALTH ---
                # Bloco dinÃ¢mico: resolve tanto a V1 quanto a V2 da biblioteca
                try:
                    # Tenta a importaÃ§Ã£o do padrÃ£o tradicional (V1.x)
                    from playwright_stealth import stealth_async
                    await stealth_async(page)
                    print("[OK] Playwright Stealth (V1) aplicado com sucesso.")
                except ImportError:
                    # Fallback para o novo padrÃ£o da biblioteca (V2.x+)
                    from playwright_stealth import Stealth
                    await Stealth().apply_stealth_async(page)
                    print("[OK] Playwright Stealth (V2) aplicado com sucesso.")
                # -------------------------------------------------

                print("Acessando pÃ¡gina de teste para scraping...")
                await page.goto("https://example.com", wait_until="networkidle")

                html_content = await page.content()
                soup = BeautifulSoup(html_content, 'lxml')
                titulo = soup.find('h1').text

                print(f"[SUCESSO] Navegador renderizou a pÃ¡gina de teste!")
                print(f"[SUCESSO] Texto capturado do elemento H1: '{titulo}'")

                await browser.close()

        # Executa a rotina assÃ­ncrona do Playwright
        asyncio.run(rodar_navegador_teste())

        print("\n====== DIAGNÃ“STICO FINALIZADO COM SUCESSO ======")

    # Ativa a execuÃ§Ã£o da tarefa definida no decorator
    checar_componentes_e_ambiente()


# Instancia a DAG para que o Airflow faÃ§a a leitura do arquivo
dag_final = teste_imagem_base_scraping_hoerlle_dag()