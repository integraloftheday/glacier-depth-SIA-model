FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        build-essential \
        ca-certificates \
        curl \
        git \
        nodejs \
        npm \
        tini \
    && npm install -g @openai/codex \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["sleep", "infinity"]
