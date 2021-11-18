FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VERSION=1.1

WORKDIR /app

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN pip3 install "poetry==$POETRY_VERSION"
RUN poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-dev --no-root

COPY . .

EXPOSE 8000
CMD ["./wait-for-it.sh", "db:3306", "--", "strawberry", "server", "app"]