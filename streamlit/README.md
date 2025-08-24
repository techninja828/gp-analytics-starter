# Streamlit + Athena (AWS ECS Fargate)

## Dashboard Tabs

| Tab | Metrics | Requirement |
| --- | --- | --- |
| Segmentation | CLV bands and RFM segments by loyalty status | Identify distinct customer segments by purchase behavior and loyalty status |
| Churn | Days since last order, average interval, spend change % | Highlight customers at risk of churn based on inactivity and spend trends |
| Sales Trends | Monthly revenue and order volume | Track seasonal sales patterns for planning |
| Loyalty | Average order value and lifetime value for loyalty members | Compare loyalty members vs non-members to evaluate program effectiveness |
| Locations | Revenue, AOV, repeat rate, orders per week | Rank restaurant locations and surface top performers |
| Discounts | Revenue and orders from discounted transactions | Measure discount impact on net sales |

## Local
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export AWS_REGION=<your-region>
export ATHENA_WORKGROUP=primary
export ATHENA_S3_STAGING_DIR=s3://gp-data-project/athena-results/
streamlit run app.py

## Container
docker build -t streamlit-athena:latest .

## Push to ECR (outline)
aws ecr create-repository --repository-name streamlit-athena
docker tag streamlit-athena:latest <acct>.dkr.ecr.<region>.amazonaws.com/streamlit-athena:latest
aws ecr get-login-password | docker login --username AWS --password-stdin <acct>.dkr.ecr.<region>.amazonaws.com
docker push <acct>.dkr.ecr.<region>.amazonaws.com/streamlit-athena:latest

## ECS Fargate steps (summary)
- Create ECS cluster (Fargate).
- Task definition:
  - Image: your ECR URL
  - Port: 8501
  - Env: AWS_REGION, ATHENA_WORKGROUP, ATHENA_S3_STAGING_DIR
  - Task **execution role**: ECR pull
  - Task **role**: Athena/Glue/S3 permissions (below)
- Service:
  - Public subnets, ALB target group on 8501
  - SG allow 80/443 (or 443 only) from your IPs
  - ACM cert on ALB for HTTPS
- S3: create s3://gp-data-project/athena-results/ for query results

## Minimal IAM policy (Task Role) – scope to your ARNs
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": ["athena:StartQueryExecution","athena:GetQueryExecution","athena:GetQueryResults","athena:StopQueryExecution","athena:GetWorkGroup"], "Resource": "*" },
    { "Effect": "Allow", "Action": ["glue:GetDatabase","glue:GetDatabases","glue:GetTable","glue:GetTables","glue:GetPartitions","glue:SearchTables"], "Resource": "*" },
    { "Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::gp-data-project"] },
    { "Effect": "Allow", "Action": ["s3:GetObject","s3:PutObject"], "Resource": ["arn:aws:s3:::gp-data-project/*"] }
  ]
}

## Notes
- App Runner doesn't support WebSockets (Streamlit relies on them), use ECS Fargate + ALB.
