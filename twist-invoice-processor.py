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

MAX_WAIT_SECONDS = 300
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

    return {
        "JobId": job_id,
        "JobStatus": "SUCCEEDED",
        "DocumentMetadata": document_metadata,
        "Warnings": warnings,
        "ExpenseDocuments": expense_documents,
    }

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
    return results
