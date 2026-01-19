# BLS Data Pipeline

**Automated serverless pipeline** that fetches BLS economic data + population statistics daily at **2AM Sydney time**.

| Component | Location |
|-----------|----------|
| **3 Lambda Functions** | Lambda → `BlsDataFetcher`, `PopulationDataFetcher`, `AnalyticsProcessor` |
| **EventBridge** | Rules → `DailyFetchRule` (2AM Sydney time daily) |
| **SQS** | `bls-pipeline-complete-AnalyticsQueue-*` |
| **S3** | `aws-s3-bls` bucket |


## 💰 Cost
**$0/month** - Lambda (720 reqs), S3 (10MB), EventBridge, SQS all Free Tier.

## 📁 Structure
```
cdk/
├── app.py
├── part4_stack.py
├── pipeline-stack.py
├── requirements.txt
├── stage.py
lambda/
├── bls_sync/
│   └── blsDataFetcher.py
├── population_sync/
│   └── populationDataFetcher.py
└── analytics/
    └── analytics.py

```
