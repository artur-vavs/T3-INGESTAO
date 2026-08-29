# Trabalho Final — Ingestão Moderna de Dados: sample_mflix

Pipeline de ingestão que extrai as 6 coleções do banco `sample_mflix` (MongoDB
Atlas) e materializa em uma camada Bronze (Delta Lake / Unity Catalog), com
landing intermediária, rastreabilidade completa, carga incremental com
watermark persistida, e orquestração via Databricks Job.

## Arquitetura

Fluxo geral:

```
MongoDB Atlas → MongoExtractor (lotes) → Landing (Volume, JSON) → BronzeWriter (parse + merge) → Bronze (Delta)
                                                                                                        ↓
                                                                                          ControlLog → control_ingestion_log
```

Componentes principais:

| Componente | Arquivo | Responsabilidade |
|---|---|---|
| `MongoExtractor` | `src/extractor` | Conecta ao Atlas, extrai por lotes com projection, filtro de watermark e retry/backoff |
| `LandingWriter` | `src/landing_writer` | Grava cada lote como JSON Lines na landing (`_source_id`, `_body`) |
| `BronzeWriter` | `src/bronze_writer` | Lê da landing, faz parse do `_body`, tipa os campos e grava/mescla na Bronze |
| `ControlLog` | `src/control_log` | Lê o último watermark e registra cada execução na tabela de controle |

## Estrutura do repositório

```
trabalho-final/
├── README.md
├── config/
│   ├── config.yaml       ← configuração da pipeline (sem credenciais)
│   └── job.yaml           ← definição do Databricks Job (orquestração)
├── notebooks/
│   ├── create_secret      ← setup único: grava a connection string no Secrets (Lógica e codificação aproveitadas pelo que foi fornecido nas aulas)
│   ├── create_schema      ← setup único: cria schema e Volume de landing
│   ├── run_ingestion       ← orquestra extração → landing → Bronze → log
│   ├── run_reconciliation ← validações de qualidade pós-carga
│   └── validation          ← checklist automatizado de entrega (R1–R8)
└── src/
    ├── extractor           ← MongoExtractor
    ├── landing_writer       ← LandingWriter
    ├── bronze_writer        ← BronzeWriter
    └── control_log          ← ControlLog
```

## Como executar

### 1. Pré-requisitos
- Workspace Databricks com Unity Catalog habilitado
- Cluster/compute Serverless
- Cluster gratuito M0 do MongoDB Atlas com o banco `sample_mflix` carregado

### 2. Setup único (rodar uma vez)

**Criar o secret com a connection string do Atlas:**
```
Abra notebooks/create_secret, preencha o widget mongo_uri, execute.
Depois: Clear Outputs antes de fazer commit.
OBSERVAÇÃO: O uri está hardcoded pois foi aproveitado o código disponibilizado pelo professor na sala de aula.
```

**Criar o schema e o Volume de landing:**
```
Abra notebooks/create_schema e execute (catalog=mongo_db, schema=bronze).
Ao rodar run_ingestion, o notebook create_schema também é executado de forma idempotente.
```

### 3. Configuração
Edite `config/config.yaml` com o catalog, database e paths do seu ambiente.
Nenhuma credencial fica nesse arquivo — apenas parâmetros de pipeline
(`modo_carga`, `campo_watermark`, `chave`, `campos`, `batch_size` por coleção).

### 4. Rodar a ingestão
```
Execute notebooks/run_ingestion
```
Processa as 6 coleções com o mesmo código genérico, gravando em landing e
depois na Bronze.

### 5. Validar
```
Execute notebooks/run_reconciliation   → checa nulos e duplicidade
Execute notebooks/validation           → checklist automatizado (R1–R8)
```

### 6. Orquestração via Job
A definição do Job está em `config/job.yaml`, com 2 tasks encadeadas
(`run_ingestion` → `run_reconciliation`), retry automático e notificação de
falha por e-mail. Publique via CLI:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run -t dev ingestao_sample_mflix
```

Ou crie manualmente em **Workflows → Create Job**, replicando as mesmas duas
tasks e a dependência entre elas. Ver histórico de execuções em
**Workflows → Runs**.

## Requisitos atendidos

| Requisito | Implementação |
|---|---|
| **R1** — Pipeline genérica e parametrizada | `MongoExtractor`/`LandingWriter`/`BronzeWriter`/`ControlLog` (classes reutilizáveis), tudo parametrizado via `config/config.yaml` |
| **R2** — Boas práticas de recursos | Cursor paginado (`batch_size`), projection (`campos`), retry com backoff exponencial, reuso de conexão Mongo, sem `collect()`/`toPandas()` sobre coleção grande |
| **R3** — Modos de carga + idempotência | Full para `users`/`theaters`/`sessions`/`embedded_movies`; incremental por `lastupdated`/`date` para `movies`/`comments`; `MERGE` por `(_source_id, _ingestion_date)` garante idempotência |
| **R4** — Rastreabilidade | 5 colunas de controle em toda tabela Bronze: `_ingestion_id`, `_ingestion_timestamp`, `_source_path`, `_load_type`, `_ingestion_date` |
| **R5** — Tabela de controle | `bronze.control_ingestion_log`, gravada a cada execução via `ControlLog` |
| **R6** — Camada Bronze fiel à origem | Delta Lake, tabelas gerenciadas via Unity Catalog, particionadas por `_ingestion_date`, nomenclatura `catalog.schema.tabela` |
| **R7** — Schema drift | `primitivesAsString` (tolera tipo divergente entre docs) + `mergeSchema` (tolera campo novo entre execuções) + coluna `_rescued_data` (quarentena, nunca descarta) |
| **R8** — Reconciliação | Comparação `qtd_lida_origem` × `qtd_gravada_destino` a cada execução; `PARTIAL` se divergência > limiar configurável |

## Decisões técnicas principais

- **Landing como buffer intermediário**: grava `_source_id` + `_body` (JSON
  serializado) em formato fixo antes de qualquer parse — evita erros de
  inferência de schema do Spark sobre dados schema-less do MongoDB.
- **Tipagem parcial na Bronze**: campos primitivos ficam como string
  (`primitivesAsString`); conversão para tipos finais (numérico, data) fica
  para a camada Silver.
- **Compatibilidade com Serverless**: pipeline evita RDD (`.rdd.map`) e
  `.cache()`/`.persist()` (não suportados em Spark Connect), usando somente
  operações de DataFrame.
- **Tabelas gerenciadas**: Bronze não usa `.option("path", ...)` — deixa o
  Unity Catalog gerenciar o armazenamento físico, evitando conflitos de
  Volume/permissão.

## Limitações conhecidas

- Tipagem final (numérico, data) não ocorre na Bronze — responsabilidade da
  Silver.
- Limiar de reconciliação é global, não configurável por coleção.
- Landing não possui rotina automática de expurgo/retenção.

## Credenciais

Nenhuma credencial está versionada. A connection string do MongoDB Atlas é
armazenada via Databricks Secrets (`notebooks/create_secret`, executado
manualmente, fora do fluxo de ingestão).

## Evidências de execução

Três execuções documentadas (prints/saída de query sobre
`control_ingestion_log`):
1. Carga full inicial (todas as coleções)
2. Carga incremental sem novidades (`qtd_lida_origem = 0`)
3. Carga incremental com dados novos inseridos manualmente no Atlas

## Contribuições

Ver `CONTRIBUICOES.md`.
