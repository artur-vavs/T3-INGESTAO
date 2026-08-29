# Registro de Contribuições

## Grupo: T3-INGESTAO

| Membro | Matrícula | Contribuições principais |
|--------|-----------|---------------------------|
| Artur Vinicius Araujo Vieira de Sousa | 2650332 | Desenho da arquitetura (landing + Bronze); implementação de `src/extractor.py` (extração paginada, projection, retry/backoff), `src/landing_writer.py`, `src/bronze_writer.py` (parse, tipagem, `MERGE` idempotente) e `src/control_log.py`; configuração (`config/config.yaml`); notebooks `run_ingestion`, `run_reconciliation`, `create_schema`, `create_secret`, `validation`; orquestração via Databricks Job (`config/job.yaml`); documentação (`README.md`, `docs/ARQUITETURA.md`); testes de idempotência e schema drift; correção de erros de compatibilidade com Serverless |

> Projeto desenvolvido individualmente — grupo composto por 1 integrante.

## Detalhamento por commit

Histórico extraído diretamente do GitHub (Repositório → Commits), já que o
desenvolvimento foi feito via Databricks Repos, sem terminal Git local:

```
artur-vavs — Add files via upload                                    (d095ce0)
artur-vavs — Add files via upload                                    (9700aa1)
artur-vavs — Add files via upload                                    (4c0b0cb)
artur-vavs — Add files via upload                                    (13052d8)
arturvavs  — Comitando os notebooks ingestion, reconciliation,
             validation, bronze_writer, control_log                  (7f1b715)
```

Todos os commits têm como autor `artur-vavs`/`arturvavs` (mesmo responsável,
Artur Vinicius Araujo Vieira de Sousa — matrícula 2650332), confirmando que a
totalidade do código do repositório foi desenvolvida por este integrante.
