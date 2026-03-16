# Sprint: Expansao de Colunas por Base

**Data**: 2026-03-16
**Objetivo**: Garantir que todas as colunas de negocio sejam extraidas do Jira para cada base (ingressadas, analisadas, encerradas), persistidas em CSV/Parquet e SQL Server.

---

## Problema Atual

O sistema extrai apenas **11 colunas normalizadas** (issue_key, summary, status, created, updated, base_origem, data_referencia, espaco, tipo_ticket, extracted_at, source_mode) para todas as bases. O negocio precisa de **24-25 colunas custom** por base, cada uma com schema diferente.

---

## Colunas Requeridas por Base

### INGRESSADAS (24 colunas)
| # | Coluna |
|---|--------|
| 1 | DATA DE ABERTURA |
| 2 | TEMA |
| 3 | ANALISTA |
| 4 | DEFINICAO DA ACAO |
| 5 | ACAO REALIZADA |
| 6 | ACAO DO RESPONSAVEL |
| 7 | N DA ORDEM |
| 8 | DATA INGRESSO ORDEM |
| 9 | TIPOLOGIA DA ORDEM |
| 10 | DATA DA ACAO/ ENVIO AREA |
| 11 | DATA DO RETORNO DA AREA |
| 12 | DATA FINALIZACAO DA ORDEM |
| 13 | 1 PRAZO POSTERGADO AO CLIENTE |
| 14 | DATA COMPROMISSO |
| 15 | DATA ULTIMA ANALISE |
| 16 | DATA DE ENTRADA |
| 17 | PRAZO |
| 18 | DESTINATARIO |
| 19 | AREA PENDENTE |
| 20 | DATA ATENDIMENTO COMPROMISSO |
| 21 | COMPROMISSO GERADO: |
| 22 | CAUSA RAIZ |
| 23 | DATA FECHOU SALESFORCE |
| 24 | ASSUNTO PRINCIPAL |

### ANALISADAS (21 colunas)
| # | Coluna |
|---|--------|
| 1 | Data limite |
| 2 | 1 PRAZO POSTERGADO AO CLIENTE |
| 3 | TEMA |
| 4 | OFICIO (somente consultorias) |
| 5 | PENDENCIA DO CASO |
| 6 | AREA PENDENTE |
| 7 | N DA ORDEM |
| 8 | N ORDEM INGRESSADA (OUV) |
| 9 | NUMERO DA ORDEM |
| 10 | Itens associados |
| 11 | DATA COMPROMISSO |
| 12 | CAUSA RAIZ |
| 13 | RESULTADO |
| 14 | CONTA CONTRATO |
| 15 | NUMERO DO PONTO DE FORNECIMENTO |
| 16 | REGIONAL |
| 17 | ANALISTA |
| 18 | MUNICIPALIDADE |
| 19 | RELATO |
| 20 | DATA ULTIMA ANALISE |
| 21 | DATA DE ABERTURA |

### ENCERRADAS (25 colunas)
| # | Coluna |
|---|--------|
| 1 | DATA DE ABERTURA |
| 2 | TEMA |
| 3 | ANALISTA |
| 4 | DEFINICAO DA ACAO |
| 5 | ACAO REALIZADA |
| 6 | ACAO DO RESPONSAVEL |
| 7 | N DA ORDEM |
| 8 | DATA INGRESSO ORDEM |
| 9 | TIPOLOGIA DA ORDEM |
| 10 | DATA DA ACAO/ ENVIO AREA |
| 11 | DATA DO RETORNO DA AREA |
| 12 | DATA FINALIZACAO DA ORDEM |
| 13 | 1 PRAZO POSTERGADO AO CLIENTE |
| 14 | DATA COMPROMISSO |
| 15 | DATA ULTIMA ANALISE |
| 16 | DATA DE ENTRADA |
| 17 | PRAZO |
| 18 | DESTINATARIO |
| 19 | AREA PENDENTE |
| 20 | DATA ATENDIMENTO COMPROMISSO |
| 21 | COMPROMISSO GERADO: |
| 22 | CAUSA RAIZ |
| 23 | DATA FECHOU SALESFORCE |
| 24 | ASSUNTO PRINCIPAL |
| 25 | FaixaDiasUteis_Simples |

---

## Checklist de Implementacao

### 1. Utilitario de Canonicalizacao de Nomes
**Arquivo**: `extractor/utils.py`
- [ ] Criar `canonicalize_column_name(display_name: str) -> str` que converte nomes Jira para nomes seguros Python/SQL
  - `"DATA FECHOU SALESFORCE"` -> `"data_fechou_salesforce"`
  - `"N DA ORDEM"` -> `"n_da_ordem"`
  - `"COMPROMISSO GERADO:"` -> `"compromisso_gerado"`
- [ ] Testes unitarios cobrindo todos os ~50 nomes unicos das 3 bases
- [ ] Verificar que nao ha colisao de nomes canonicalizados entre bases

### 2. Registro de Colunas por Base
**Arquivo**: `extractor/business_rules.py`
- [ ] Adicionar campo `custom_fields: tuple[str, ...]` ao dataclass `BaseRule`
- [ ] Definir tupla INGRESSADAS_FIELDS (24 campos)
- [ ] Definir tupla ANALISADAS_FIELDS (21 campos)
- [ ] Definir tupla ENCERRADAS_FIELDS (25 campos)
- [ ] Popular `custom_fields` em cada entrada do dict `RULES`
- [ ] Criar funcao `all_required_field_names() -> set[str]` (uniao de todos os custom_fields)
- [ ] Atualizar/substituir `REQUIRED_FIELD_NAMES` para usar a nova funcao

### 3. Validacao Dinamica por Base
**Arquivo**: `extractor/validators.py`
- [ ] Extrair `CORE_COLUMNS` (as 11 colunas originais do sistema) como constante
- [ ] Criar `get_required_columns(base: BaseName) -> tuple[str, ...]` (core + custom canonicalizados)
- [ ] Atualizar `validate_records()` para receber parametro `base: BaseName`
- [ ] Validacao not-null apenas para core columns (custom fields aceitam None)

### 4. Normalizacao Dinamica de Campos
**Arquivo**: `extractor/normalizer.py`
- [ ] `normalize_api_issues()`: iterar sobre `rule.custom_fields`, extrair via `field_ids[name]` + `_pick_scalar()`, usar nome canonicalizado como chave
- [ ] Manter as 11 core columns intactas + adicionar custom fields ao dict de saida
- [ ] `normalize_fallback_csv()`: usar `pick_column(name)` para cada campo em `rule.custom_fields`
- [ ] Campos ausentes no CSV do fallback -> None (nao falhar)

### 5. Persistencia em Arquivo (CSV/Parquet)
**Arquivo**: `extractor/storage.py`
- [ ] Substituir import de `REQUIRED_COLUMNS` por `get_required_columns(base)`
- [ ] Ajustar `persist_processed()` para usar colunas dinamicas no `reindex()`
- [ ] Manter deduplicacao por `(issue_key, updated)`

### 6. Persistencia em SQL Server
**Arquivo**: `extractor/sql_server_writer.py`
- [ ] Criar dataclass `ColumnDef(sql_name, sql_type, record_key, converter)`
- [ ] Criar metodo `_get_column_defs(base) -> list[ColumnDef]` (core + custom)
- [ ] Custom fields defaultam para `NVARCHAR(4000) NULL`
- [ ] Gerar `CREATE TABLE` dinamicamente a partir dos column defs
- [ ] Gerar `INSERT` e `DELETE` SQL dinamicamente
- [ ] Refatorar `_build_rows()` para construir tuplas dinamicamente
- [ ] Implementar migracao de schema: consultar `INFORMATION_SCHEMA.COLUMNS` e executar `ALTER TABLE ADD` para colunas novas
- [ ] Tabelas existentes nao perdem dados (colunas novas sao NULL)

### 7. Orquestracao do Servico
**Arquivo**: `extractor/service.py`
- [ ] Alterar `resolve_field_ids()` para usar `all_required_field_names()` em vez de `REQUIRED_FIELD_NAMES`
- [ ] Construir `fields` tuple dinamicamente: core Jira fields + `[field_ids[f] for f in rule.custom_fields]`
- [ ] Passar `base` para `validate_records()`
- [ ] Tratar campos nao resolvidos com log warning (nao bloqueia extracao)

### 8. Testes
- [ ] `tests/test_normalizer.py` — Testar extracao de todos os custom fields por base (API e fallback)
- [ ] `tests/test_validators.py` (novo) — Testar validacao por base, core vs custom
- [ ] `tests/test_business_rules.py` (novo) — Testar `all_required_field_names()`, ausencia de colisao na canonicalizacao
- [ ] `tests/test_sql_server_writer.py` (novo) — Testar geracao dinamica de schema e migracao ALTER TABLE
- [ ] `tests/test_service.py` — Ajustar mocks para novo contrato de fields expandidos

### 9. Smoke Test End-to-End
- [ ] Rodar `poetry run extractor-run --base ingressadas --mode api-first --format csv` e verificar CSV com 24+ colunas
- [ ] Rodar para analisadas e verificar 21+ colunas
- [ ] Rodar para encerradas e verificar 25+ colunas
- [ ] Verificar tabelas SQL Server com schema expandido (`SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'jira_ingressadas'`)
- [ ] Verificar que fallback Playwright tambem retorna colunas expandidas

---

## Ordem de Execucao
```
utils.py -> business_rules.py -> validators.py -> normalizer.py -> storage.py -> sql_server_writer.py -> service.py -> testes -> smoke test
```

---

## Riscos e Mitigacoes

| Risco | Mitigacao |
|-------|----------|
| Campo custom nao resolve no Jira (nome errado/renomeado) | Log warning + preencher com None, nao bloqueia extracao |
| Colisao de nomes canonicalizados | Teste automatizado para todos os ~50 nomes unicos |
| Tabelas SQL existentes com schema antigo (14 colunas) | ALTER TABLE ADD e seguro, colunas novas recebem NULL nos registros antigos |
| Fallback CSV nao inclui todos os campos | Campos ausentes viram None |
| Jira API rate limit com mais campos por request | Os campos sao retornados na mesma chamada search, sem requests adicionais |

---

## Arquivos Criticos

| Arquivo | Impacto |
|---------|---------|
| `extractor/utils.py` | Nova funcao de canonicalizacao |
| `extractor/business_rules.py` | Registro de colunas por base |
| `extractor/validators.py` | Validacao dinamica |
| `extractor/normalizer.py` | Extracao de todos os campos |
| `extractor/storage.py` | Reindex dinamico |
| `extractor/sql_server_writer.py` | Schema e migracao dinamicos |
| `extractor/service.py` | Orquestracao expandida |
| `extractor/interfaces.py` | Sem mudanca necessaria |

---

## Definition of Done
- Todas as colunas listadas acima aparecem nos CSVs de saida para cada base
- Todas as colunas existem nas tabelas SQL Server correspondentes
- Testes unitarios passam
- Smoke test end-to-end validado para as 3 bases
- Nenhuma regressao nas funcionalidades existentes (core columns, fallback, audit)
