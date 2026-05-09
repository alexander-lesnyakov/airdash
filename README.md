# Airdash

A minimal terminal UI for Apache Airflow.

On first launch, the app asks for:

- Airflow URL, for example `https://airflow.example.com`
- Bearer token

It stores those values in the user config directory and opens directly to the DAG list on later runs.

## Quick Launch

```bash
python -m pip install -e .
airdash
```

## Run

```bash
airdash
```

## Features

- First-run setup screen for URL and token
- Persistent local config file
- DAG table with pause state, schedule, next run time, latest run status, latest run time, and last 10 run statuses
- DAG run table with status filters
- Switch between DAGs and DAG runs with `v`
- Update Airflow URL or bearer token with `c`
- Explicit auth failure message when the saved token is expired, invalid, revoked, or unauthorized
- Filter DAG runs with `1` failed, `2` running, `3` queued, `4` success, `0` all
- Select rows with `Enter` or `Space`
- Trigger selected DAGs with `t`
- Mark selected DAG runs successful with `s`
- Mark selected DAGs' latest runs successful with `s` and a run count, defaulting to `10`
- Refresh with `r`
- Quit with `q`

The Airflow URL should be the base webserver URL. If `/api/v2` is included, the app strips it before configuring `apache-airflow-client`.

## Token Expiration

Airflow REST API tokens expire according to `[api_auth] jwt_expiration_time`. The Airflow default is `86400` seconds, or 24 hours. When a saved token expires, press `c` in `airdash` and paste a newly generated token.

If your Airflow security policy allows longer-lived bearer tokens, an Airflow administrator can set a 30-day expiration in `airflow.cfg`:

```ini
[api_auth]
jwt_expiration_time = 2592000
```
