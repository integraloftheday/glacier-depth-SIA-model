# Docker Dev Setup

This setup creates a development container for this project with:
- Port mapping: container `8080` -> host `8884`
- Mounted workspace: `./workspace` -> `/workspace`
- Codex installed in the container (`@openai/codex`)
- Host Codex config reuse: `${HOME}/.codex` mounted to `/workspace/.codex`
- Host skills reuse: `${HOME}/.agents` mounted to `/workspace/.agents`

## Build and Run

```bash
HOST_UID=$(id -u) HOST_GID=$(id -g) docker compose up -d --build
```

## Open a shell in the container

```bash
docker compose exec glacier-dev bash
```

## Verify Codex is available and already authenticated

```bash
docker compose exec glacier-dev bash -lc 'codex --version && ls -la /workspace/.codex'
```

## Stop and remove container

```bash
docker compose down
```
