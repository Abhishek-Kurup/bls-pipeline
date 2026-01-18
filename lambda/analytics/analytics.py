import boto3
import pandas as pd
import io
import os
s3 = boto3.client('s3')


def handler(event, context):
    for record in event['Records']:
        bucket = os.environ['S3_BUCKET']
        bls_key = os.environ['BLS_S3_PREFIX']
        pop_key = os.environ['POP_S3_PREFIX']

        # Load datasets
        bls = s3.get_object(Bucket=bucket, Key=bls_key)
        pop = s3.get_object(Bucket=bucket, Key=pop_key)

        bls_df = pd.read_csv(io.BytesIO(bls['Body'].read()), sep='\t')
        pop_df = pd.read_json(io.BytesIO(pop['Body'].read()))

        # Strip whitespace from column names
        bls_df.columns = bls_df.columns.str.strip()

        # Strip whitespace from string values
        bls_df = bls_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Report 1: Population stats 2013-2018
        pop_stats = pop_df[(pop_df['Year'] >= 2013) & (pop_df['Year'] <= 2018)]['Population']
        print(f"REPORT 1: Mean={pop_stats.mean():.0f}, Std={pop_stats.std():.0f}")

        # Report 2: Best year per series (sample)
        bls_df['value_num'] = pd.to_numeric(bls_df['value'], errors='coerce')
        bls_df['year'] = bls_df['period'].str[:4]
        yearly_max = bls_df.groupby(['series_id', 'year'])['value_num'].sum().groupby(level=0).max()
        print(f"REPORT 2: Sample series PRS30006011 best year sum: {yearly_max.get('PRS30006011', 'N/A')}")

        # Report 3: PRS30006032 Q01
        prs_q01 = bls_df[(bls_df['series_id'] == 'PRS30006032') & (bls_df['period'].str.endswith('Q01'))]
        print(f"REPORT 3: PRS30006032 Q01 rows: {len(prs_q01)}")

    return {'statusCode': 200}
