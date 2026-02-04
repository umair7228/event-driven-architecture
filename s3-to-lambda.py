import json
import boto3
import csv
from io import StringIO
from datetime import datetime

s3_client = boto3.client('s3')


def lambda_handler(event, context):
    print(event)

    bucket_name = event['Records'][0]['s3']['bucket']['name']
    input_file_key = event['Records'][0]['s3']['object']['key']

    if not input_file_key.startswith('input_data/'):
        return {"statusCode": 200, "body": "Skipped non-input file"}

    try:
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=input_file_key
        )

        file_content = response['Body'].read().decode('utf-8')
        csv_file = StringIO(file_content)
        reader = csv.DictReader(csv_file)

        fieldnames = reader.fieldnames + ['close_pct_change', 'created_at']
        output_csv = StringIO()
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            row['date'] = transform_date(row['date'])
            row['volume'] = adjust_volume(row['volume'])

            if float(row['low']) == 0:
                row['low'] = str(
                    (float(row['open']) + float(row['close'])) / 2
                )

            row['close_pct_change'] = recalculate_pct_change(
                row['open'], row['close']
            )

            row['created_at'] = datetime.utcnow().isoformat()
            writer.writerow(row)

        output_file_key = input_file_key.replace('input_data/', 'output_data/')

        s3_client.put_object(
            Bucket=bucket_name,
            Key=output_file_key,
            Body=output_csv.getvalue()
        )

        return {
            "statusCode": 200,
            "body": f"Processed: {output_file_key}"
        }

    except Exception as e:
        print(e)
        return {"statusCode": 500, "body": str(e)}


# ---------------- HELPER FUNCTIONS ---------------- #

def transform_date(date_str):
    """Convert MM/DD/YYYY → YYYY-MM-DD"""
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except Exception:
        return date_str


def adjust_volume(volume):
    """If volume < 100,000 → N/A"""
    try:
        return 'N/A' if int(volume) < 100000 else volume
    except Exception:
        return volume


def recalculate_pct_change(open_price, close_price):
    """((close - open) / open) * 100"""
    try:
        open_price = float(open_price)
        close_price = float(close_price)

        if open_price == 0:
            return '0'

        return str(((close_price - open_price) / open_price) * 100)

    except Exception:
        return '0'
