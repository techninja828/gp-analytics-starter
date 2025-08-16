
# GP Analytics Starter

End-to-end bits for your AWS lakehouse + Streamlit.

## Layout
- `glue/jobs/` — Glue ETL (bronze→silver, gold)
- `athena/ddl/` — external tables
- `athena/views/` — BI views (6 dashboards)
- `streamlit/` — Streamlit app, Dockerfile
- `.github/workflows/deploy.yml` — GitHub Actions to ECR/ECS
- `infra/ecs/taskdef.json` — ECS Fargate task definition (edit ARNs, image, env)

## Quick start
1) Update S3 bucket names and ARNs in SQL/JSON/YAML.
2) Push repo to GitHub.
3) Create ECR repo (`streamlit-athena`), ECS cluster/service behind ALB.
4) Set up GitHub OIDC role in AWS, put its ARN in the workflow.
5) Push to `main` → CI builds & deploys.

## Athena config
Set `ATHENA_S3_STAGING_DIR` env var to your S3 results path (and grant ECS task role access).
