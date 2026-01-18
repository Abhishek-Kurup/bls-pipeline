import aws_cdk as cdk
from pipeline_stack import MyPipelineStack

app = cdk.App()
MyPipelineStack(app, "MyPipelineStack")
app.synth()
