# Extração de Bases Jira (API-First com Fallback Playwright)

## 1) Objetivo
Este documento define a estratégia operacional para extrair diariamente 3 bases do Jira com prioridade na API (Jira Cloud REST) e fallback via Playwright apenas quando a API falhar.

Escopo da extração:
- Base `encerradas`
- Base `analisadas`
- Base `ingressadas`

Decisões fixas deste projeto:
- Stack de referência: `Python`
- Saída principal: `CSV` e `Parquet` em disco
- Janela temporal padrão: de `(D-1) - 1 mês` até `D-1`.
- Estratégia: `api-first`
- Fallback via Playwright: somente em falha de API

---

## 2) Visão Geral do Fluxo
1. Calcular a janela de execução (`from`, `to`), padrão `(D-1)-1 mês .. D-1`.
2. Resolver IDs de campos customizados no Jira por nome (sem hardcode de `customfield_xxxxx`).
3. Montar JQL por base com regras de status/tipo/espaço e data de referência.
4. Consultar issues via API com paginação (`startAt`, `maxResults`).
5. Persistir:
   - bruto (`jsonl`)
   - normalizado (`csv` e `parquet`) com schema dinâmico por base
6. Validar qualidade de dados (schema, datas, volume mínimo quando aplicável).
7. Registrar auditoria da execução.
8. Em falha de API (após retry), acionar Playwright para exportação via UI e padronizar no mesmo contrato de dados.

---

## 3) Regras de Negócio (Consolidadas)

### 3.1 Base `encerradas`
- Filtro Jira: `https://ouvid.atlassian.net/issues/?filter=10719`
- Campo de data de referência: `DATA FECHOU SALESFORCE`
- Regra de status: `STATUS = ENCERRADO`
- Regra adicional: `Espaço = Atendimento Ouv`

### 3.2 Base `analisadas`
- Filtro Jira: `https://ouvid.atlassian.net/issues/?filter=10720`
- Campo de data de referência: `DATA ÚLTIMA ANÁLISE`
- Regra de status (lista permitida):
  - `ABERTO`, `ANALISAR`, `ANÁLISE`, `CIRADO`, `DESIGNADA`, `DEVOLVIDO`,
  - `EM ABERTO`, `EM TRATAMENTO`, `EM TRATATIVA`, `EM VERIFICAÇÃO`, `EM ANDAMENTO`,
  - `PENDENTE RETORNO`, `POSTERGADO`, `RECLASSIFICAR`, `RESPONDIDO`, `RETORNO`,
  - `REITERADAS EM ABERTO`, `RECORRER ANEEL`, `PROCEDENTES`
- Regra adicional: `Tipo do ticket = ATENDIMENTO`
- Observação operacional: essa base pode exigir reprocessamento, pois inclui itens ainda em tratamento.

### 3.3 Base `ingressadas`
- Filtro Jira: `https://ouvid.atlassian.net/issues/?filter=10721`
- Campo de data de referência: `DATA DE ABERTURA`
- Regra adicional: `Espaço = Atendimento Ouv`

### 3.4 Regra temporal comum
- Janela padrão diária:
  - `from = (D-1) - 1 mês`
  - `to = D-1`
  - Exemplo em `2026-03-03`: `from=2026-02-02`, `to=2026-03-02`

---

## 4) Pré-Requisitos
- Python `3.11` até `3.13` (recomendado `3.13`)
- Poetry `2.2+`
- Acesso ao Jira Cloud com permissão de leitura nos filtros/boards envolvidos
- Credenciais Jira (`email` + `api_token`)
- Dependências Python esperadas:
  - `requests` (ou `httpx`)
  - `pandas`
  - `pyarrow`
  - `python-dotenv`
  - `tenacity` (ou retry equivalente)
  - `playwright` (fallback)
- Para Playwright:
  - `poetry run playwright install`

---

## 5) Configuração de Ambiente (`.env`)
Use placeholders e injete segredos em runtime (CI/CD, secret manager, vault).

```dotenv
JIRA_BASE_URL=https://ouvid.atlassian.net
JIRA_EMAIL=seu-email@empresa.com
JIRA_API_TOKEN=seu_token_aqui

OUTPUT_DIR=./output
TIMEZONE=America/Bahia
MAX_RESULTS=100
RETRY_ATTEMPTS=4
RETRY_BACKOFF_SECONDS=2
PLAYWRIGHT_HEADLESS=true
CLEAN_OUTPUT_ON_API_RUN=true
LOG_LEVEL=INFO
LOG_JSON=false
LOG_FILE=output/logs/application.log
LOG_MAX_BYTES=10485760
LOG_BACKUP_COUNT=10

DB_ENABLED=true
DB_SERVER=10.71.202.120
DB_PORT=1433
DB_DRIVER=ODBC Driver 18 for SQL Server
DB_DATABASE=db
DB_USER=userdb
DB_PASSWORD=trocar-por-senha-real
DB_SCHEMA=dbo
DB_ENCRYPT=false
DB_TRUST_SERVER_CERTIFICATE=true
DB_CONNECT_TIMEOUT=30
```

Importante:
- Não versionar credenciais reais.
- Tratar o arquivo `Documentação bases Jira.docx` como sensível, pois contém segredos em texto.
- Com `CLEAN_OUTPUT_ON_API_RUN=true`, os diretórios `output/raw/<base>`, `output/processed/<base>` e `output/fallback/<base>` são limpos antes de cada execução `api-first`.
- Com `DB_ENABLED=true` e variáveis `DB_*` válidas, os dados de cada base também são carregados no SQL Server.
- Para evitar timeout de login no SQL Server, use `DB_SERVER` como host/IP (sem `\\instancia`) e informe `DB_PORT` explicitamente.

---

## 6) Interface de Execução (CLI)
CLI proposta:

```bash
poetry run extractor-run \
  --base all|encerradas|analisadas|ingressadas \
  --from YYYY-MM-DD \
  --to YYYY-MM-DD \
  --mode api-first \
  --format csv,parquet
```

Comportamentos:
- `--from/--to` omitidos: usar padrão `(D-1)-1 mês .. D-1`
- `--mode api-first`: tenta API; fallback Playwright apenas em erro elegível
- `--base all`: processa as 3 bases na mesma execução

Exemplos:

```bash
# Janela padrão ((D-1)-1 mês .. D-1), todas as bases
poetry run extractor-run --base all --mode api-first --format csv,parquet

# Janela explícita
poetry run extractor-run --base analisadas --from 2026-02-01 --to 2026-03-02 --mode api-first
```

---

## 7) Estratégia API (Principal)

### 7.1 Autenticação
- Jira Cloud REST com Basic Auth (`email:api_token`).
- Base URL: `${JIRA_BASE_URL}/rest/api/3`.

### 7.2 Descoberta dinâmica de campos customizados
Endpoint:
- `GET /rest/api/3/field`

Resolver por nome exato (case-insensitive controlado):
- `DATA FECHOU SALESFORCE`
- `DATA ÚLTIMA ANÁLISE`
- `DATA DE ABERTURA`
- `Espaço`
- `Tipo do ticket`

Regra:
- Falhar cedo (`fail-fast`) se qualquer campo obrigatório não for encontrado.

### 7.3 Montagem de JQL por base
Padrão:
- aplicar filtro da janela no campo de data da base
- aplicar regras fixas de status/tipo/espaço
- ordenar por data de referência e `key`

Exemplo lógico de JQL (`encerradas`):

```text
"Espaço" = "Atendimento Ouv"
AND status = "ENCERRADO"
AND "DATA FECHOU SALESFORCE" >= "2026-02-02"
AND "DATA FECHOU SALESFORCE" <= "2026-03-02"
ORDER BY "DATA FECHOU SALESFORCE" ASC, key ASC
```

### 7.4 Paginação
Endpoint:
- `GET /rest/api/3/search`

Parâmetros mínimos:
- `jql`
- `fields` (somente colunas necessárias)
- `startAt`
- `maxResults`

Processo:
1. `startAt = 0`
2. Buscar página
3. Acrescentar resultados
4. `startAt += maxResults`
5. Repetir até `startAt >= total`

### 7.5 Persistência
Para cada base e período de execução (`from_date`..`to_date`):
- Bruto:
  - `raw/<base>/<from_date>__<to_date>.jsonl`
- Normalizado:
  - `processed/<base>/<from_date>__<to_date>.csv`
  - `processed/<base>/<from_date>__<to_date>.parquet`
  - `processed/<base>/periodo_<from_date>__<to_date>.json`
  - Cada base mantém as 11 colunas core do sistema e adiciona suas colunas custom de negócio.
  - `ingressadas`: 35 colunas totais (11 core + 24 custom)
  - `analisadas`: 32 colunas totais (11 core + 21 custom)
  - `encerradas`: 36 colunas totais (11 core + 25 custom)

### 7.6 Idempotência
Chave técnica recomendada:
- `dedup_key = issue_key + updated`

Regras:
- Se mesma `dedup_key` reaparecer no mesmo ciclo de carga, manter apenas 1 registro.
- Em reprocessamento da base `analisadas`, substituir snapshot do dia ou gravar partição com versionamento explícito.

### 7.7 Validação de qualidade
Mínimos:
- Schema obrigatório presente
- `data_referencia` dentro de `from..to`
- Sem nulos em chaves críticas (`issue_key`, `status`, `updated`)
- Sinalizar (warning) volume zero quando historicamente improvável

### 7.8 Auditoria
Registrar por base:
- `timestamp_execucao`
- `base`
- `from_date`
- `to_date`
- `query_hash`
- `total_extraido`
- `duracao_ms`
- `source_mode` (`api`)
- `status_execucao`
- `erro` (se houver)

### 7.9 Carga SQL Server
Após persistir os arquivos em `output/processed`, a aplicação:
1. Conecta no SQL Server via `DB_SERVER/DB_PORT/DB_DRIVER/DB_DATABASE/DB_USER/DB_PASSWORD`.
2. Cria automaticamente as tabelas (se não existirem):
   - `dbo.jira_encerradas`
   - `dbo.jira_analisadas`
   - `dbo.jira_ingressadas`
3. Mantém colunas core em português na base de dados:
   - `chave_ticket`, `resumo`, `status`, `data_criacao`, `data_atualizacao`, `base_origem`,
   - `data_referencia`, `espaco`, `tipo_ticket`, `extraido_em`, `modo_origem`,
   - `periodo_inicio`, `periodo_fim`, `carga_em`
4. Adiciona automaticamente colunas custom por base usando nomes canonicalizados (ex.: `data_fechou_salesforce`, `tema`, `faixa_dias_uteis_simples`).
5. Em tabelas já existentes, novas colunas são criadas com `ALTER TABLE ADD` sem apagar dados antigos.
4. Reescreve o período atual (`DELETE` por `periodo_inicio/periodo_fim`) e insere os dados novos.
5. Valida veracidade comparando contagem inserida vs contagem existente no período.

---

## 8) Estratégia Fallback Playwright (Somente Erro de API)

### 8.1 Critérios de acionamento
Acionar fallback apenas quando API falhar após retries:
- HTTP `401`, `403`, `429`, `5xx`
- Timeout total por base
- Erro de schema/resposta da API que impeça parse

### 8.2 Fluxo do fallback
1. Abrir URL de filtro da base (`10719`, `10720`, `10721`).
2. Garantir autenticação/sessão Jira válida.
3. Executar exportação:
   - Preferência: CSV completo
   - Alternativa quando aplicável: fluxo de export via UI (ex.: Apps > Open in Microsoft Excel)
4. Salvar artefato em:
   - `fallback/<base>/<run_date>/`
5. Transformar dados exportados para o mesmo contrato de colunas da API.
6. Rodar mesmas validações de qualidade.
7. Registrar auditoria com:
   - `source_mode = playwright_fallback`
   - `fallback_reason`

### 8.3 Restrições do fallback
- Não usar fallback como rotina principal.
- Não mascarar falhas sistêmicas de credenciais/permissão: registrar erro com causa raiz.
- Em caso de falha simultânea API + fallback, execução deve terminar com status de erro.

---

## 9) Contrato de Dados (Normalizado)
Colunas mínimas obrigatórias:
- `issue_key`
- `summary`
- `status`
- `created`
- `updated`
- `base_origem`
- `data_referencia`
- `espaco`
- `tipo_ticket`
- `extracted_at`
- `source_mode`

Regras de tipo:
- Datas/timestamps em ISO-8601 com timezone
- `source_mode` em enum: `api | playwright_fallback`
- `base_origem` em enum: `encerradas | analisadas | ingressadas`

---

## 10) Estrutura de Diretórios de Saída

```text
output/
  raw/
    encerradas/
      2026-02-02__2026-03-02.jsonl
    analisadas/
      2026-02-02__2026-03-02.jsonl
    ingressadas/
      2026-02-02__2026-03-02.jsonl
  processed/
    encerradas/
      2026-02-02__2026-03-02.csv
      2026-02-02__2026-03-02.parquet
      periodo_2026-02-02__2026-03-02.json
    analisadas/
      2026-02-02__2026-03-02.csv
      2026-02-02__2026-03-02.parquet
      periodo_2026-02-02__2026-03-02.json
    ingressadas/
      2026-02-02__2026-03-02.csv
      2026-02-02__2026-03-02.parquet
      periodo_2026-02-02__2026-03-02.json
  fallback/
    analisadas/
      2026-03-03/
        export.csv
  logs/
    execution.log
  audit/
    extraction_audit_2026-03-03.jsonl
```

---

## 11) Operação Diária e Agendamento

### 11.1 Frequência recomendada
- 1 execução diária.

### 11.2 Exemplo com `cron`
Executar todos os dias às 06:10 (timezone do servidor):

```cron
10 6 * * * cd /caminho/projeto && poetry run extractor-run --base all --mode api-first --format csv,parquet >> output/logs/cron.log 2>&1
```

### 11.3 Observabilidade
Monitorar:
- latência por base
- taxa de sucesso API
- acionamento de fallback (deve ser baixo)
- volume extraído por base
- percentual de registros descartados por deduplicação
- erros por endpoint HTTP (`/v1/extractions/run`)
- taxa de limpeza de diretórios por execução (`api-first`)

Alertas recomendados:
- 2+ dias consecutivos com fallback acionado
- volume zero inesperado
- erro de autenticação repetido

Logs da aplicação:
- Console e arquivo rotativo são habilitados por padrão.
- Arquivo padrão: `output/logs/application.log`
- Rotação por tamanho:
  - `LOG_MAX_BYTES`
  - `LOG_BACKUP_COUNT`
- Formato:
  - texto (default)
  - JSON (`LOG_JSON=true`) para ingestão em observabilidade.

Acompanhar logs em Linux:

```bash
tail -f output/logs/application.log
```

---

## 12) Tratamento de Erros e Reprocessamento

### 12.1 Retry/backoff
Recomendação:
- `RETRY_ATTEMPTS=4`
- backoff exponencial com jitter

### 12.2 Reprocessamento
Casos:
- correção de regra de negócio
- indisponibilidade temporária da API
- necessidade de revisão de base `analisadas`

Prática:
- permitir rerun para mesma janela
- manter rastreabilidade por `query_hash`, `run_id`, `source_mode`

### 12.3 Falhas bloqueantes
Encerrar com erro quando:
- variáveis obrigatórias ausentes
- campos customizados não encontrados
- API e fallback indisponíveis
- schema final inválido

---

## 13) Testes e Critérios de Aceite
1. API extrai `encerradas`, `analisadas` e `ingressadas` gerando CSV e Parquet.
2. Paginação funciona com múltiplas páginas.
3. Janela padrão `(D-1)-1 mês .. D-1` é respeitada quando `--from/--to` não são informados.
4. Base `analisadas` respeita a lista de status e `Tipo do ticket = ATENDIMENTO`.
5. Falha simulada de API aciona fallback automaticamente.
6. Saída do fallback é padronizada no mesmo schema da API.
7. Reprocessamento não gera duplicidade por `issue_key + updated`.
8. Auditoria registra origem, duração, volume e motivo de fallback.
9. Pipeline falha com mensagem clara para credenciais ausentes/inválidas.

---

## 14) Segurança e Conformidade
- Nunca armazenar credenciais reais em arquivos versionados.
- Usar secret manager ou variáveis injetadas pelo ambiente.
- Restringir permissões de leitura dos logs e artefatos.
- Tratar arquivos de documentação com segredos como material confidencial.

---

## 15) Assumptions e Defaults
- Jira alvo: `Jira Cloud` (`/rest/api/3`)
- Implementação de referência: `Python`
- Saída principal: arquivo local `CSV + Parquet`
- Fallback somente quando API falhar
- Janela padrão: `(D-1)-1 mês .. D-1`
- Timezone padrão: `America/Bahia`
- Mapeamento de campos customizados por nome (dinâmico), sem hardcode de `customfield_xxxxx`

---

## 16) Implementação Entregue
Estrutura atual do projeto:

```text
api/
  main.py
  schemas.py
extractor/
  audit.py
  bootstrap.py
  business_rules.py
  config.py
  domain.py
  exceptions.py
  interfaces.py
  jira_api_client.py
  jql_builder.py
  normalizer.py
  playwright_fallback.py
  run.py
  service.py
  storage.py
  validators.py
docs/modules/README.md
tests/
  test_api.py
  test_jql_builder.py
  test_normalizer.py
  test_service.py
Dockerfile
docker-compose.yml
```

---

## 17) Execução Local
Instalação:

```bash
poetry install --with dev
```

Preparar ambiente:

```bash
cp .env.example .env
```

Executar via CLI:

```bash
poetry run extractor-run --base all --mode api-first --format csv,parquet
```

Executar API FastAPI:

```bash
poetry run extractor-api
```

Se o projeto foi instalado com `--no-root`, use:

```bash
poetry run python -m api.main
```

Endpoints:
- `GET /healthz`
- `POST /v1/extractions/run`

Exemplo de request:

```bash
curl -X POST 'http://localhost:8000/v1/extractions/run' \
  -H 'Content-Type: application/json' \
  -d '{
    "base": "all",
    "mode": "api-first",
    "formats": ["csv", "parquet"]
  }'
```

---

## 18) Testes Automatizados
Executar:

```bash
poetry run pytest
```

Cobertura atual:
- construção de JQL
- normalização de payload API
- fallback automático no serviço quando API falha
- endpoint FastAPI de execução

---

## 19) Containerização para Produção
Build da imagem:

```bash
docker build -t jira-extractor-api:latest .
```

Run local com Docker:

```bash
docker run --rm -p 8000:8000 --env-file .env -v $(pwd)/output:/app/output jira-extractor-api:latest
```

Run com Docker Compose:

```bash
docker compose up -d --build
```

Observações:
- O container expõe a API em `8000`.
- Artefatos de saída são persistidos no volume `./output`.
# extract-from-jira
