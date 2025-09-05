"""
AWS Lambda handler that uses Textract StartExpenseAnalysis for PDFs in S3.

Usage patterns:
- S3 event trigger: pass the standard S3 Put event; handler extracts bucket/key.
- Direct invoke: {"bucket": "my-bucket", "key": "path/to/file.pdf", "wait": true}
  - If "wait" is true (default: false), the function will poll Textract for up to
    MAX_WAIT_SECONDS (env, default 300) and return the full results.
  - If "wait" is false, it returns the JobId immediately.

Note: StartExpenseAnalysis only accepts S3 inputs (not bytes). Use this for PDFs/TIFFs.
"""

from __future__ import annotations
import json
import os
import time
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import BotoCoreError, ClientError

textract_client = boto3.client("textract")
s3_client = boto3.client("s3")

def _get_s3(event: Dict[str, Any]) -> Tuple[str, str]:
    """Extract S3 bucket and key from an S3 event or direct payload."""
    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object.")

    # S3 event shape
    try:
        records = event.get("Records", [])
        if records and isinstance(records, list):
            s3 = records[0].get("s3")
            if s3 and isinstance(s3, dict):
                bucket = s3["bucket"]["name"]
                key = urllib.parse.unquote_plus(s3["object"]["key"])
                return bucket, key
    except Exception:
        pass

    # Direct invocation
    bucket = event.get("bucket")
    key = event.get("key")
    if bucket and key:
        return str(bucket), str(key)

    raise ValueError(
        "Provide S3 bucket/key via S3 event or payload {bucket, key}."
    )

def _write_to_s3(expense_document: Any, *,
                 output_bucket: str = "twist-invoices-processed",
                 output_key: str) -> None:
    body = json.dumps(expense_document, ensure_ascii=False, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=body,
        ContentType="application/json"
    )

def start_analysis(bucket: str, key: str,
                           notification_sns_arn: Optional[str] =
                           "arn:aws:sns:us-west-2:624248574390:twist-invoices-notifs",
                           notification_role_arn: Optional[str] =
                           "arn:aws:iam::624248574390:role/twist-invoice-textract"
                           ) -> str:
    """Start an asynchronous Textract expense analysis job and return JobId."""
    params: Dict[str, Any] = {
        "DocumentLocation": {"S3Object": {"Bucket": bucket, "Name": key}}
    }
    if notification_sns_arn and notification_role_arn:
        params["NotificationChannel"] = {
            "SNSTopicArn": notification_sns_arn,
            "RoleArn": notification_role_arn,
        }

    response = textract_client.start_expense_analysis(**params)
    print(response)
    return response["JobId"]


def wait_analysis(job_id: str,
                             max_wait_seconds: int = 300,
                             poll_interval_seconds: int = 5) -> Dict[str, Any]:
    """Poll Textract until the job completes, then return aggregated results."""
    deadline = time.time() + max_wait_seconds
    status: Optional[str] = None

    while time.time() < deadline:
        resp = textract_client.get_expense_analysis(JobId=job_id)
        status = resp.get("JobStatus")
        if status in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
            break
        time.sleep(poll_interval_seconds)

    if status != "SUCCEEDED":
        return {
            "JobId": job_id,
            "JobStatus": status or "UNKNOWN",
            "StatusMessage": resp.get("StatusMessage") if 'resp' in locals() else None,
        }

    # Gather all pages
    expense_documents: List[Dict[str, Any]] = []
    document_metadata: Optional[Dict[str, Any]] = None
    warnings: Optional[List[Dict[str, Any]]] = None
    next_token: Optional[str] = None

    while True:
        if next_token:
            page = textract_client.get_expense_analysis(JobId=job_id, NextToken=next_token)
        else:
            page = textract_client.get_expense_analysis(JobId=job_id)

        document_metadata = page.get("DocumentMetadata") or document_metadata
        warnings = page.get("Warnings") or warnings
        expense_documents.extend(page.get("ExpenseDocuments", []))
        next_token = page.get("NextToken")
        if not next_token:
            break

    while True:
        if next_token:
            page = textract_client.get_expense_analysis(JobId=job_id, NextToken=next_token)
        else:
            page = textract_client.get_expense_analysis(JobId=job_id)

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
        "ExpenseDocuments": processed_output,
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

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda entrypoint.

    Expects either an S3 event or direct payload {bucket, key, wait?}.
    Optional env:
      - WAIT_FOR_COMPLETION=true|false (default false)
      - MAX_WAIT_SECONDS=300
      - NOTIFY_SNS_ARN, NOTIFY_ROLE_ARN for SNS notifications (optional)
    """
    bucket, key = _get_s3(event)

    notify_sns_arn = "arn:aws:sns:us-west-2:624248574390:twist-invoices-notifs"
    notify_role_arn = "arn:aws:iam::624248574390:role/twist-invoice-textract"

    try:
        job_id = start_analysis(
            bucket=bucket,
            key=key,
            notification_sns_arn=notify_sns_arn,
            notification_role_arn=notify_role_arn,
        )
    except (ClientError, BotoCoreError) as error:
        return {
            "ok": False,
            "error": str(error),
            "bucket": bucket,
            "key": key,
        }

    should_wait = bool(
        str(event.get("wait", os.getenv("WAIT_FOR_COMPLETION", "false"))).lower()
        in ("1", "true", "yes")
    )

    if not should_wait:
        return {"ok": True, "JobId": job_id, "bucket": bucket, "key": key}

    try:
        max_wait_seconds = int(os.getenv("MAX_WAIT_SECONDS", "300"))
    except ValueError:
        max_wait_seconds = 300

    results = wait_analysis(job_id=job_id, max_wait_seconds=max_wait_seconds)
    results.update({"ok": results.get("JobStatus") == "SUCCEEDED", "bucket": bucket, "key": key})
    # _write_to_s3(results.get("ExpenseDocuments"), output_key=key.removesuffix(".pdf")+".json")
    return results
