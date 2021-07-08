ARG PYTHON_IMAGE_TAG=latest

FROM python:${PYTHON_IMAGE_TAG}  AS image_stage

ARG APP_TAG="1.0.3"

LABEL \
  org.label-schema.build-date=Now \
  org.label-schema.maintainer="m1ha@carrotquest.io" \
  org.label-schema.schema-version="1.0.0-rc1" \
  org.label-schema.vcs-ref="v${APP_TAG}" \
  org.label-schema.vcs-url="https://github.com/carrotquest/django-clickhouse" \
  org.label-schema.vendor="Carrot quest" \
  org.label-schema.version="${APP_TAG}"

ENV APP_UID ${APP_UID:-1000}
ENV APP_GID ${APP_GID:-1000}
ENV APP_NAME ${APP_NAME:-"app"}

# Configure utf-8 locales to make sure Python correctly handles unicode filenames
# Configure pip local path to copy data from pip_stage
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 DJANGO_SETTINGS_MODULE=tests.settings PYTHONUSERBASE=/pip PATH=/pip/bin:$PATH

RUN set -eu && \
  groupadd --gid "${APP_GID}" "app" && \
  useradd --uid ${APP_UID} --gid ${APP_GID} --create-home --shell /bin/bash -d /app app && \
  mkdir -p /pip && \
  chmod  755 /app /pip && \
  chown -R ${APP_UID}:${APP_GID} /app /pip

WORKDIR /app/src

# Install dependencies
# set -eu "breaks" pipeline on first error
COPY ./requirements-test.txt /app/requirements-test.txt
RUN --mount=type=cache,target=/root/.cache/pip \
  set -eu && \
  python3 -m pip install --upgrade pip setuptools wheel  && \
  python3 -m pip install --upgrade --requirement /app/requirements-test.txt

COPY . /app/src

RUN python3 setup.py -q install --user

USER ${APP_UID}

CMD ["python3", "runtests.py"]