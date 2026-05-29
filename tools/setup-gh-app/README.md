# setup-gh-app

One-shot helper that runs the GitHub App **manifest flow** end-to-end:
you click **Create GitHub App** once in the browser, and this CLI handles
everything else (form prefill, code exchange, credential storage, kubectl
command generation).

## Usage

```bash
go build -o setup-gh-app .
./setup-gh-app
```

By default it:

- Reads the cluster's existing `paas-system/github-webhook` secret so the
  webhook URL matches what's already deployed
- Listens on a random localhost port and opens your browser
- Submits a pre-filled manifest to GitHub
- Catches the redirect, exchanges the code at
  `POST /app-manifests/{code}/conversions`
- Writes `app-id`, `client-id`, `client-secret`, `webhook-secret`,
  `private-key.pem`, and `install-url` into `./out/`
- Prints the exact `kubectl` commands to reconcile the cluster

Override any field with flags — `./setup-gh-app -h` for the full list.

## Why this exists

GitHub Apps cannot be created via a personal access token. The
[manifest flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest)
is the official programmatic path, but it requires a browser redirect.
This tool is the smallest possible shim around it.

## Security note

The `out/` directory contains the App's private key. It's gitignored,
but you should delete it as soon as you've loaded the secrets into
the cluster.
