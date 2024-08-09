import boto3
import time
from dotenv import load_dotenv, find_dotenv
import os
import re
load_dotenv(find_dotenv())


def extract_text_from_pdf(pdf_file_path):
    # Upload the PDF file to S3
    access_key = os.environ.get('aws_access_key')
    secret_access_key = os.environ.get('aws_secret_access_key')
    bucket_name = os.environ.get('bucket_name')
    object_name = pdf_file_path
    s3_client = boto3.client('s3', region_name='ap-south-1', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    s3_client.upload_file(pdf_file_path, bucket_name, object_name)

    # Initialize the Textract client
    textract_client = boto3.client('textract', region_name='ap-south-1', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

    # Start the Textract analysis
    response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': object_name
            }
        }
    )

    # Get the JobId from the response
    job_id = response['JobId']

    # Poll the Textract job status
    while True:
        result = textract_client.get_document_text_detection(JobId=job_id)
        status = result['JobStatus']

        if status in ['SUCCEEDED', 'FAILED']:
            break

        print(f"Job status: {status}")
        time.sleep(5)  # Adjust the polling interval as needed

    # Check if the job was successful
    if status == 'SUCCEEDED':
        # Extract and return the text
        extracted_text = {}
        next_token = None
        while True:
            if next_token:
                result = textract_client.get_document_text_detection(JobId=job_id, NextToken=next_token)
            else:
                result = textract_client.get_document_text_detection(JobId=job_id)

            for item in result['Blocks']:
                if item['BlockType'] == 'LINE':
                    page_number = item['Page']
                    text = item['Text']
                    if page_number not in extracted_text:
                        extracted_text[page_number] = ""
                    extracted_text[page_number] += text + '\n'

            next_token = result.get('NextToken')
            if not next_token:
                break
        combined_text = ""
        for page_number in extracted_text.keys():
            if page_number in extracted_text:
                combined_text += f"Page {page_number}:\n{extracted_text[page_number]}\n"
        return combined_text.strip()
    else:
        # Handle the case where the job failed
        print(f"Textract job failed with status: {status}")
        return None

