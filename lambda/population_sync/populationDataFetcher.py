import json
import boto3
import urllib.request

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    population_config = event.get("CONFIG", None)
    if not population_config:
        raise Exception("Config data not passed")

    api_url = population_config.get("url", None)

    try:
        # Fetch data using urllib
        print(f"Fetching data from API URL: {api_url}")
        with urllib.request.urlopen(api_url) as response:
            data = json.loads(response.read().decode())

        file_name = f"datausa_population.json"

        bucket_name = event.get("BUCKET_NAME", None)
        s3_prefix = population_config.get("s3_prefix", "")

        if not bucket_name:
            raise ValueError("BUCKET_NAME data not passed")

        print(f"Saving data to S3 bucket: {bucket_name}//{s3_prefix}{file_name}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=f"{s3_prefix}{file_name}",
            Body=json.dumps(data),
            ContentType="application/json"
        )

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data successfully saved to S3",
                "bucket": bucket_name,
                "file": file_name
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }