import aws_cdk as cdk
from aws_cdk import pipelines
from constructs import Construct
from stage import MyLambdaStage

class MyPipelineStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        pipeline = pipelines.CodePipeline(self, "Pipeline",
            synth=pipelines.ShellStep("Synth",
                input=pipelines.CodePipelineSource.connection(
                    "Abhishek-Kurup/bls-pipeline", # Update this
                    "main",
                    connection_arn="arn:aws:codeconnections:ap-southeast-2:224486859603:connection/5cbb254c-c56e-4922-b2bd-f2ce9f67d0f3"
                ),
                commands=[
                    "pip install -r requirements.txt",
                    "npx cdk synth"
                ]
            )
        )

        # Add the stage that contains your Lambdas
        pipeline.add_stage(MyLambdaStage(self, "Dev"))
