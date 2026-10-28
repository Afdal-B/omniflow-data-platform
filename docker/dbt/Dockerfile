FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:0.12.15 /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    PATH=/opt/venv/bin:$PATH

WORKDIR /app
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --frozen --only-group dbt --no-install-project

RUN useradd --create-home --uid 10001 dbt && install -d -o dbt /app/dbt_project
USER dbt

WORKDIR /app/dbt_project
COPY --chown=dbt dbt_project/packages.yml dbt_project/dbt_project.yml ./
RUN dbt deps

COPY --chown=dbt dbt_project/ ./

ENV DBT_PROFILES_DIR=/app/dbt_project \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs
ENTRYPOINT ["dbt"]
CMD ["build"]
