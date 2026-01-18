from aws_cdk import Stage
from constructs import Construct
from part4_stack import CompletePipelineStack # Import your original stack class

class MyLambdaStage(Stage):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)
        # This instantiates your 3 Lambda functions and events
        CompletePipelineStack(self, "LambdaDeploymentStack")
