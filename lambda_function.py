import json
import boto3
import csv
from io import StringIO
from datetime import datetime


s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print(event)
    # Extract the bucket name and file key from the event
    sqs_message_body = json.loads(event['Records'][0]['body'])
    
    # Now extract the bucket name and file key from the parsed S3 event
    bucket_name = sqs_message_body['Records'][0]['s3']['bucket']['name']
    input_file_key = sqs_message_body['Records'][0]['s3']['object']['key']
    
    print(f"Bucket Name: {bucket_name}")
    print(f"File Key: {input_file_key}")

    
    # Ensure we are only processing files from the 'input/' folder
    if input_file_key.startswith('input/'):
        try:

            response = s3_client.get_object(Bucket=bucket_name, Key=input_file_key)
            file_content = response['Body'].read().decode('utf-8')
            
            csv_file = StringIO(file_content)
            reader = csv.DictReader(csv_file)
            fieldnames = reader.fieldnames +['close_pct_change'] + ['created_at']

            output_csv = StringIO()
            writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in reader:

                row['date'] = transform_date(row['date'])
                
                row['volume'] = adjust_volume(row['volume'])
                
                if float(row['low']) == 0:
                    row['low'] = str((float(row['open']) + float(row['close'])) / 2)
                
                row['close_pct_change'] = recalculate_pct_change(row['open'], row['close'])
                
                row['created_at'] = datetime.now()
                
                writer.writerow(row)

            # Create the output file key
            output_file_key = input_file_key.replace('input/', 'output/')
            
            s3_client.put_object(
                Bucket=bucket_name,
                Key=output_file_key,
                Body=output_csv.getvalue()  
            )
            
            return {
                'statusCode': 200,
                'body': json.dumps(f'File processed and uploaded to {output_file_key}')
            }
        except Exception as e:
            print(f"Error processing file: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error: {str(e)}")
            }


def transform_date(date_str):
    """Convert date from MM/DD/YYYY to YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return date_str  

def adjust_volume(volume):
    """If volume is less than 100,000, set it to 'N/A'."""
    try:
        if int(volume) < 100000:
            return 'N/A'
        return volume
    except ValueError:
        return volume  

def recalculate_pct_change(open_price, close_price):
    try:
        open_price = float(open_price)
        close_price = float(close_price)

        if open_price == 0:
            return '0'  
        else:
            return str(((close_price - open_price) / open_price) * 100)
    except Exception as e:
        print(f"Error calculating percentage change: {e}")
        return '0'  

def clean_time_format(time_str):
    """Example transformation function to clean up the 'created_at' column."""
    try:
        time_parts = time_str.split(':')
        return f"{time_parts[0].zfill(2)}:{time_parts[1].zfill(2)}"
    except:
       return time_str
