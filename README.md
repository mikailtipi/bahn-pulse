# Bahn Pulse

A bitemporal data warehouse for German rail.

Deutsche Bahn publishes a delay forecast for each train many times before it arrives.
Bahn Pulse stores every forecast version and measures how accurate those forecasts are,
and how the error shrinks as the train gets closer.

**Status:** work in progress. Full documentation follows once the pipeline runs.

![Architecture](docs/architecture.png)

## Development

```bash
uv sync
uv run pre-commit install
uv run pytest
```

## Infrastructure

Terraform lives in [`infra/terraform`](infra/terraform):

| Folder | Purpose |
|---|---|
| `bootstrap/` | S3 bucket for Terraform state (S3 native locking, no DynamoDB). Its own state lives in that bucket. |
| `modules/s3_bucket/` | Private bucket with secure defaults: no public access, SSE-S3, TLS only, versioning with expiry. |
| `live/` | Main stack: bronze bucket for raw API responses. |

```bash
export AWS_PROFILE=bahn-pulse
cd infra/terraform/live
terraform init
terraform plan
```

## License

MIT, see [LICENSE](LICENSE). Data sources keep their own licenses.
