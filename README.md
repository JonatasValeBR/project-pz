# Plataforma Airflow PGFN

Projeto autônomo para hospedar DAGs da PGFN. A infraestrutura local é
compartilhada; cada integração deve manter DAGs, código Python, Connections,
pools e diretórios de dados claramente isolados.

## Conteúdo atual

- Airflow 3.3.0 com CeleryExecutor, PostgreSQL e Redis;
- DAG principal e DAG de recuperação seletiva do TRF3/MNI;
- DAG `trf1_process_list`, que gera a lista diária do TRF1 sem baixar PDFs;
- pacote Python `src/trf3_mni`;
- pacote Python `src/trf1_scraping`;
- testes unitários, contratuais e fixtures offline;
- object storage uniforme com `fsspec`: filesystem local no desenvolvimento e
  backend `gs://` preparado para o bucket corporativo;
- dados, manifestos e logs preservados da implantação anterior.

## Estrutura

```text
dags/                 DAGs de todos os fluxos
src/                  regras de negócio e adaptadores por integração
tests/                testes sem rede real
data/<dag_id>/         artefatos persistidos e isolados por DAG
docker-compose.yaml    ambiente Airflow local compartilhado
```

O arquivo `.env` é local e não deve ser versionado. Novos tribunais não devem
criar outro cluster automaticamente: adicione o pacote e as DAGs correspondentes
a esta plataforma, salvo quando houver requisito explícito de isolamento.

O destino dos artefatos é configurado por `PGFN_ARTIFACT_STORE_URI`. O padrão do
Compose é `file:///opt/airflow/data`; no cluster será
`gs://<bucket>/<prefixo>`, sem mudança na lógica das DAGs.
