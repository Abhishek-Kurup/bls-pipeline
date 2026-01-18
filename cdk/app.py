#!/usr/bin/env python3
import aws_cdk as cdk
from part4_stack import CompletePipelineStack

app = cdk.App()
CompletePipelineStack(app, "BlsPipelineStack", env=cdk.Environment(region="ap-southeast-2"))
app.synth()
