# README Setup - Jira Extractor API-First

## 1) Objetivo
Este guia centraliza tudo que precisa ser instalado para rodar o projeto localmente em Linux com:
- `pyenv`
- `poetry`
- dependencias de sistema
- bibliotecas Python
- Playwright (fallback)
- SQL Server ODBC Driver (`pyodbc`)
- Docker e Docker Compose

## 2) Versoes suportadas
- Python: `>=3.11,<3.14` (recomendado `3.12.x` ou `3.13.x`)
- Poetry: `2.2+`
- Projeto: FastAPI + Uvicorn + Pandas + PyArrow + Playwright + PyODBC

Importante:
- Nao usar Python `3.14` neste projeto (pode falhar build de dependencias como `pyarrow`).

## 3) Dependencias de sistema (Linux)
```bash
sudo apt-get update
sudo apt-get install -y \
  build-essential curl git ca-certificates pkg-config \
  make gcc g++ unixodbc unixodbc-dev \
  libssl-dev zlib1g-dev libbz2-dev libreadline-dev \
  libsqlite3-dev libffi-dev liblzma-dev xz-utils tk-dev
```

## 4) Instalar pyenv
```bash
curl https://pyenv.run | bash
```

Adicionar no `~/.bashrc`:
```bash
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"
```

Recarregar shell:
```bash
source ~/.bashrc
```

Instalar Python e fixar no projeto:
```bash
pyenv install 3.12.10
pyenv local 3.12.10
python --version
```

## 5) Instalar Poetry
```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry --version
```

## 6) Instalar dependencias Python do projeto
No diretorio do projeto:
```bash
poetry env use $(pyenv which python)
poetry install --with dev
```

Pacotes principais instalados via Poetry:
- `fastapi`
- `uvicorn[standard]`
- `requests`
- `pandas`
- `pyarrow`
- `python-dotenv`
- `pydantic`
- `pydantic-settings`
- `tenacity`
- `playwright`
- `pyodbc`
- `black`
- `pytest`, `pytest-cov`, `httpx`

## 7) Instalar browser do Playwright
```bash
poetry run playwright install --with-deps chromium
```

## 8) Instalar ODBC Driver 18 para SQL Server
### Ubuntu
```bash
source /etc/os-release
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | \
  sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -sSL "https://packages.microsoft.com/config/ubuntu/${VERSION_ID}/prod.list" | \
  sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### Debian/Kali
```bash
source /etc/os-release
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | \
  sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl -sSL "https://packages.microsoft.com/config/debian/${VERSION_ID}/prod.list" | \
  sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

Verificar drivers:
```bash
odbcinst -q -d
```

## 9) Instalar Docker e Compose
```bash
sudo apt-get install -y docker.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
docker --version
docker compose version
```

## 10) Configurar variaveis de ambiente
```bash
cp .env.example .env
```

Preencher no `.env`:
- Jira:
  - `JIRA_BASE_URL`
  - `JIRA_EMAIL`
  - `JIRA_API_TOKEN`
- App:
  - `OUTPUT_DIR`
  - `TIMEZONE`
  - `MAX_RESULTS`
  - `RETRY_ATTEMPTS`
  - `RETRY_BACKOFF_SECONDS`
  - `PLAYWRIGHT_HEADLESS`
- Banco:
  - `DB_ENABLED=true`
  - `DB_SERVER` (host/IP, sem `\instancia`)
  - `DB_PORT` (ex.: `1433`)
  - `DB_DRIVER=ODBC Driver 18 for SQL Server`
  - `DB_DATABASE`
  - `DB_USER`
  - `DB_PASSWORD`
  - `DB_SCHEMA`

## 11) Executar projeto
### API
```bash
poetry run uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### CLI
```bash
poetry run extractor-run --base all --mode api-first --format csv,parquet
```

### Requisicao HTTP (3 bases + ingest DB no fluxo)
```bash
curl -X POST 'http://localhost:8000/v1/extractions/run' \
  -H 'Content-Type: application/json' \
  -d '{
    "base": "all",
    "mode": "api-first",
    "formats": ["csv", "parquet"]
  }'
```

## 12) Rodar testes
```bash
poetry run pytest -q
```

## 13) Rodar com Docker
Build:
```bash
docker build -t jira-extractor-api:latest .
```

Run:
```bash
docker run --rm -p 8000:8000 --env-file .env -v $(pwd)/output:/app/output jira-extractor-api:latest
```

Compose:
```bash
docker compose up --build
```

## 14) Troubleshooting rapido
### `pyarrow` falha ao instalar
- Confirme Python `3.12`/`3.13` (nao `3.14`).
- Recrie ambiente:
```bash
poetry env remove --all
poetry env use $(pyenv which python)
poetry install --with dev
```

### `Can't open lib 'ODBC Driver 18 for SQL Server'`
- Driver nao instalado corretamente.
- Reinstale `msodbcsql18`.
- Valide com `odbcinst -q -d`.

### `HYT00 Login timeout expired`
- SQL Server inacessivel por rede/porta.
- Ajuste `DB_SERVER` para host/IP sem instancia nomeada.
- Defina `DB_PORT` explicito.
- Verifique firewall/VPN/rota ate o servidor SQL.

