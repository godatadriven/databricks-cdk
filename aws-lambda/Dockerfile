# Docker image to run the datascience-resources lambda function.

FROM public.ecr.aws/lambda/python:3.9


COPY ./src/ ./src/
COPY pyproject.toml .
COPY poetry.lock .
COPY README.md .

RUN yum install -y -q gcc gcc-c++ unixODBC-devel curl netcat \
    && export POETRY_VERSION=1.1.13 \
    && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python - \
    && PATH="${PATH}:/root/.poetry/bin" \
    && poetry config virtualenvs.create false \
    && poetry install --no-dev --no-ansi \
    && yum remove -y gcc gcc-c++ unixODBC-devel \
    && rm -rf /var/lib/apt/lists/* \
    && yum clean -q all \
    && rm -rf /var/cache/yum

COPY src/databricks_cdk/handler.py ${LAMBDA_TASK_ROOT}

CMD ["handler.handler"]
