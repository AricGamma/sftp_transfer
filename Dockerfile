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
ENV NUM_WORKERS=8
ENV BATCH_SIZE=1

CMD [ "sftp_transfer", "-sh", ${SOURCE_HOST}, "-sp", ${SOURCE_PORT}, "-su", ${SOURCE_USER}, "-sp", ${SOURCE_PASSWORD}, "-sp", ${SOURCE_PATH}, "-dh", ${DESTINATION_HOST}, "-dp", ${DESTINATION_PORT}, "-du", ${DESTINATION_USER}, "-dp", ${DESTINATION_PASSWORD}, "-nw", ${NUM_WORKERS}, "-bs", ${BATCH_SIZE} ]
