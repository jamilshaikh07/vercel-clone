# paas-sample-hello

Minimal sample app used to smoke-test the self-hosted PaaS build pipeline.

- `Dockerfile` — single-stage nginx-unprivileged image (rootless, port 8080)
- `index.html` — static page that confirms the deploy worked

Built in-cluster by Kaniko, no Docker daemon required.
