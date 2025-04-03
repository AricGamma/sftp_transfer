FROM alpine:latest as builder

RUN apk add python3 py3-pip pipx

RUN pipx install pdm

ENV PATH=$PATH:/root/.local/bin

WORKDIR /app

COPY . /app/

RUN pdm build

FROM alpine:latest

RUN apk add python3 py3-pip pipx

COPY --from=builder /app/dist/*.whl /tmp

RUN pipx install /tmp/*.whl

ENV PATH=$PATH:/root/.local/bin

ENV SOURCE_HOST=""
ENV SOURCE_PORT=22
ENV SOURCE_USER=""
ENV SOURCE_PASSWORD=""
ENV SOURCE_PATH=""
ENV DESTINATION_HOST=""
ENV DESTINATION_PORT=22
ENV DESTINATION_USER=""
ENV DESTINATION_PASSWORD=""
ENV DESTINATION_PATH=""
ENV NUM_WORKERS=8
ENV BATCH_SIZE=1

WORKDIR /app

COPY "entrypoint.sh" "/app/entrypoint.sh"

CMD [ "/app/entrypoint.sh" ]
