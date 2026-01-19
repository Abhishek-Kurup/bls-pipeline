import json
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
    aws_lambda_event_sources as event_sources,
    aws_iam as iam,
    aws_s3_notifications as notifications,
    aws_s3 as s3
)
from constructs import Construct
import os

# Load secure config
with open('config.json', 'r') as f:
    config = json.load(f)

class CompletePipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = config['bucket_name']

        # Part 1: BLS Sync Lambda
        bls_lambda = _lambda.Function(
            self, "BlsDataFetcher",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="bls_sync.handler",
            code=_lambda.Code.from_asset("lambda/bls_sync"),
            timeout=cdk.Duration.minutes(15)
        )

        # Part 2: Population Lambda
        pop_lambda = _lambda.Function(
            self, "PopulationDataFetcher",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="population.handler",
            code=_lambda.Code.from_asset("lambda/population_sync"),
            timeout=cdk.Duration.minutes(5)
        )

        # Daily Schedule (2AM Sydney = 16 UTC)
        daily_schedule = events.Rule(
            self, "DailyFetchRule",
            schedule=events.Schedule.cron(minute='0', hour='16')
        )

        daily_schedule.add_target(targets.LambdaFunction(
            bls_lambda,
            event=events.RuleTargetInput.from_object({
            "BUCKET_NAME": config['bucket_name'],
            "CONFIG": config.get("bls")
        })))

        daily_schedule.add_target(targets.LambdaFunction(
            pop_lambda,
            event=events.RuleTargetInput.from_object({
            "BUCKET_NAME": config['bucket_name'],
            "CONFIG": config.get("population"),
        })))

        # Part 3: Analytics pipeline
        analytics_queue = sqs.Queue(self, "AnalyticsQueue")
        analytics_lambda = _lambda.Function(
            self, "AnalyticsProcessor",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="analytics.handler",
            code=_lambda.Code.from_asset("lambda/analytics"),
            timeout=cdk.Duration.minutes(5),
            environment={
                "BLS_BUCKET": bucket_name,
                'POP_S3_PREFIX': f"{config['population']['s3_prefix']}datausa_population.json",
                'BLS_S3_PREFIX': f"{config['population']['s3_prefix']}pr.data.0.Current",
            }
        )

        # S3 → SQS trigger (population.json)
        bucket = s3.Bucket.from_bucket_name(self, "Bucket", bucket_name)
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            notifications.SqsDestination(analytics_queue),
            s3.NotificationKeyFilter(suffix="population.json")
        )

        # SQS → Analytics Lambda
        analytics_lambda.add_event_source(event_sources.SqsEventSource(analytics_queue))

        # S3 permissions
        for fn in [bls_lambda, pop_lambda, analytics_lambda]:
            fn.add_to_role_policy(iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
                resources=[f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"]
            ))
