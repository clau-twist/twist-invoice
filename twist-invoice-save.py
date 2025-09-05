import json
import boto3
from typing import List, Dict, Any, Optional

textract_client = boto3.client("textract")
s3_client = boto3.client("s3")


def _write_to_s3(expense_document: Any, *,
                 output_bucket: str = "twist-invoices-processed",
                 output_key: str) -> None:
    body = json.dumps(expense_document, ensure_ascii=False, indent=2).encode("utf-8")
    print(f"this is expense: {expense_document}")
    print(f"this is output key: {output_key}")
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=body,
        ContentType="application/json"
    )

def get_expense(job_id):
    """
    Retrieves the expense analysis results for a given job ID.

    Parameters:
    ----------
    job_id: str, required
        The unique identifier for the expense analysis job.

    Returns:
    -------
    dict
        The expense analysis results.
    """

    textract_client = boto3.client("textract")

    # Gather all pages
    expense_documents: List[Dict[str, Any]] = []
    document_metadata: Optional[Dict[str, Any]] = None
    warnings: Optional[List[Dict[str, Any]]] = None
    next_token: Optional[str] = None

    while True:
        if next_token:
            page = textract_client.get_expense(JobId=job_id, NextToken=next_token)
        else:
            page = textract_client.get_expense(JobId=job_id)

        document_metadata = page.get("DocumentMetadata") or document_metadata
        warnings = page.get("Warnings") or warnings
        expense_documents.extend(page.get("ExpenseDocuments", []))
        next_token = page.get("NextToken")
        if not next_token:
            break

    try:
        processed_output = process_output(expense_documents)
    except Exception as e:
        print(f"Error processing: {e}")
        processed_output = expense_documents

    return {
        "JobId": job_id,
        "JobStatus": "SUCCEEDED",
        "DocumentMetadata": document_metadata,
        "Warnings": warnings,
        "ExpenseDocuments": expense_documents,
    }

def find_summary_field(summary_fields, field_type):
    """
    Helper function to find a specific field and its confidence in the SummaryFields list.
    Returns a dictionary with 'value' and 'confidence'.
    """
    for field in summary_fields:
        if field.get('Type', {}).get('Text') == field_type:
            value_detection = field.get('ValueDetection', {})
            value = value_detection.get('Text', 'N/A').strip()
            confidence = value_detection.get('Confidence', 0.0)
            return {'value': value, 'confidence': confidence}
    return {'value': 'N/A', 'confidence': 0.0}

def find_grouped_field(summary_fields, group_type, field_type):
    """
    Helper function to find grouped fields and their confidence.
    Returns a dictionary with 'value' and 'confidence'.
    """
    for field in summary_fields:
        # Check if the field has GroupProperties
        if 'GroupProperties' in field:
            for prop in field['GroupProperties']:
                # Check if the desired group_type (e.g., "VENDOR") is in this group
                if group_type in prop.get('Types', []):
                    # If the field_type matches, return the value and confidence
                    if field.get('Type', {}).get('Text') == field_type:
                        value_detection = field.get('ValueDetection', {})
                        value = value_detection.get('Text', 'N/A').strip()
                        confidence = value_detection.get('Confidence', 0.0)
                        return {'value': value, 'confidence': confidence}
    return {'value': 'N/A', 'confidence': 0.0}

def process_output(data):
    """
    Processes the raw Textract data and returns a structured list of dictionaries,
    including confidence scores for each extracted value.
    """
    processed_docs = []

    # Consolidate all summary fields from all pages for easier searching
    all_summary_fields = []
    for doc in data:
        all_summary_fields.extend(doc.get('SummaryFields', []))

    # --- Build the primary JSON object with consolidated data ---
    summary_data = {
        "invoice_id": find_summary_field(all_summary_fields, "INVOICE_RECEIPT_ID"),
        "invoice_date": find_summary_field(all_summary_fields, "INVOICE_RECEIPT_DATE"),
        "due_date": find_summary_field(all_summary_fields, "DUE_DATE"),
        "total": find_summary_field(all_summary_fields, "TOTAL"),
        "po_number": find_summary_field(all_summary_fields, "PO_NUMBER")
    }

    vendor_data = {
        "name": find_grouped_field(all_summary_fields, "VENDOR", "NAME"),
        "address": find_grouped_field(all_summary_fields, "VENDOR", "ADDRESS"),
        "tax_id": find_summary_field(all_summary_fields, "TAX_PAYER_ID")
    }

    receiver_data = {
        "name": find_grouped_field(all_summary_fields, "RECEIVER_BILL_TO", "NAME"),
        "billing_address": find_grouped_field(all_summary_fields, "RECEIVER_BILL_TO", "ADDRESS"),
        "shipping_address": find_grouped_field(all_summary_fields, "RECEIVER_SHIP_TO", "ADDRESS")
    }

    # --- Process Line Items ---
    line_items_data = []
    # Loop through documents to find the one with line items
    for doc in data:
        line_item_groups = doc.get('LineItemGroups', [])
        if line_item_groups:
            for group in line_item_groups:
                for item in group.get('LineItems', []):
                    item_details = {}
                    for field in item.get('LineItemExpenseFields', []):
                        field_type = field.get('Type', {}).get('Text', '').lower()
                        value_detection = field.get('ValueDetection', {})

                        field_value = value_detection.get('Text', 'N/A').replace('\n', ' ')
                        field_confidence = value_detection.get('Confidence', 0.0)

                        item_details[field_type] = {
                            'value': field_value,
                            'confidence': field_confidence
                        }
                    line_items_data.append(item_details)

    # --- Assemble the final JSON object ---
    final_json = {
        "summary": summary_data,
        "vendor": vendor_data,
        "receiver": receiver_data,
        "line_items": line_items_data
    }

    return final_json

def lambda_handler(event, context):
    """
    This function is triggered by an SNS notification and logs the message.

    Parameters:
    ----------
    event: dict, required
        SNS event format
        https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html
    context: object, required
        Lambda Context runtime methods and attributes
        https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns:
    ------
    dict
        A simple success message.
    """
    print("Lambda function invoked by SNS.")

    # Iterate over each record in the event. There's typically one for SNS.
    for record in event['Records']:
        sns_message = record['Sns']['Message']

        print(f"Received SNS message: {sns_message}")

        # If the SNS message is a JSON string, you can parse it
        try:
            message_data = json.loads(sns_message)
            print("Successfully parsed SNS message as JSON:")
            try:
                job_id = message_data["JobId"]
                key = message_data["DocumentLocation"]["S3ObjectName"]
                print(f" here's the jobid: {message_data["JobId"]}")
            except Exception as e:
                print(f"Error getting jobid: {e}")
                return {
                    'statusCode': 500,
                    'body': json.dumps('Error getting jobid')
                }
            # Pretty print the JSON data
        except json.JSONDecodeError:
            print("SNS message is not a valid JSON string.")
            # Handle as plain text

    results = get_analysis(job_id=job_id)
    _write_to_s3(results.get("ExpenseDocuments"), output_key=key.removesuffix(".pdf")+".json")

    return {
        'statusCode': 200,
        'body': json.dumps('SNS message processed successfully!')
    }
