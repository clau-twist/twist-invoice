import json
import boto3
import logging
from typing import List, Dict, Any, Optional

textract_client = boto3.client("textract")
s3_client = boto3.client("s3")
LOG_LEVEL = "INFO"

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

def setup_structured_logging(request_id: str, job_id: str):
    for handler in logger.handlers:
        logger.removeHandler(handler)

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        json.dumps({
            "timestamp": "%(asctime)s",
            "level": "%(levelname)s",
            "message": "%(message)s",
            "aws_request_id": request_id,
            "textract_job_id": job_id
        })
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

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

def find_summary_field(summary_fields, field_type):
    """
    Helper function to find a specific field and its confidence in the SummaryFields list.
    Returns a dictionary with "value" and "confidence".
    """
    for field in summary_fields:
        if field.get("Type", {}).get("Text") == field_type:
            value_detection = field.get("ValueDetection", {})
            value = value_detection.get("Text", "N/A").strip()
            confidence = value_detection.get("Confidence", 0.0)
            return {"value": value, "confidence": confidence}
    return {"value": "N/A", "confidence": 0.0}

def find_grouped_field(summary_fields, group_type, field_type):
    """
    Helper function to find grouped fields and their confidence.
    Returns a dictionary with "value" and "confidence".
    """
    for field in summary_fields:
        # Check if the field has GroupProperties
        if "GroupProperties" in field:
            for prop in field["GroupProperties"]:
                # Check if the desired group_type (e.g., "VENDOR") is in this group
                if group_type in prop.get("Types", []):
                    # If the field_type matches, return the value and confidence
                    if field.get("Type", {}).get("Text") == field_type:
                        value_detection = field.get("ValueDetection", {})
                        value = value_detection.get("Text", "N/A").strip()
                        confidence = value_detection.get("Confidence", 0.0)
                        return {"value": value, "confidence": confidence}
    return {"value": "N/A", "confidence": 0.0}

def process_output(data):
    """
    Processes the raw Textract data and returns a structured list of dictionaries,
    including confidence scores for each extracted value.
    """
    logger.info("Starting processing of Textract analysis.")
    processed_docs = []
    warnings = []

    # --- Helper function to check for missing values and log warnings ---
    def get_value_and_warn(value_or_dict, field_name, field_type="field"):
        """
        Validate an extracted value (string or {"value","confidence"} dict) and
        log a warning if it's missing.

        Treat empty strings and sentinel values like "N/A" as missing.
        Returns the original value/dict so callers can assign it directly.
        """
        if isinstance(value_or_dict, dict):
            extracted_value = value_or_dict.get("value")
        else:
            extracted_value = value_or_dict

        is_missing = (
            extracted_value is None
            or str(extracted_value).strip() == ""
            or str(extracted_value).strip().upper() in {"N/A", "NA", "NONE", "NULL"}
        )

        if is_missing:
            message = f"Required {field_type} '{field_name}' not found in document."
            logger.warning(message)
            warnings.append(message)

        return value_or_dict

    # Consolidate all summary fields from all pages for easier searching
    all_summary_fields = []
    for doc in data:
        all_summary_fields.extend(doc.get("SummaryFields", []))

    # --- Build the primary JSON object with consolidated data ---
    invoice_id_info = find_summary_field(all_summary_fields, "INVOICE_RECEIPT_ID")
    get_value_and_warn(invoice_id_info, "invoice_id", field_type="summary field")

    invoice_date_info = find_summary_field(all_summary_fields, "INVOICE_RECEIPT_DATE")
    get_value_and_warn(invoice_date_info, "invoice_date", field_type="summary field")

    due_date_info = find_summary_field(all_summary_fields, "DUE_DATE")
    get_value_and_warn(due_date_info, "due_date", field_type="summary field")

    total_info = find_summary_field(all_summary_fields, "TOTAL")
    get_value_and_warn(total_info, "total", field_type="summary field")

    po_number_info = find_summary_field(all_summary_fields, "PO_NUMBER")
    get_value_and_warn(po_number_info, "po_number", field_type="summary field")

    summary_data = {
        "invoice_id": invoice_id_info,
        "invoice_date": invoice_date_info,
        "due_date": due_date_info,
        "total": total_info,
        "po_number": po_number_info,
    }

    vendor_name_info = find_grouped_field(all_summary_fields, "VENDOR", "NAME")
    get_value_and_warn(vendor_name_info, "vendor.name", field_type="vendor field")

    vendor_address_info = find_grouped_field(all_summary_fields, "VENDOR", "ADDRESS")
    get_value_and_warn(vendor_address_info, "vendor.address", field_type="vendor field")

    vendor_tax_id_info = find_summary_field(all_summary_fields, "TAX_PAYER_ID")
    get_value_and_warn(vendor_tax_id_info, "vendor.tax_id", field_type="vendor field")

    vendor_data = {
        "name": vendor_name_info,
        "address": vendor_address_info,
        "tax_id": vendor_tax_id_info,
    }

    receiver_name_info = find_grouped_field(all_summary_fields, "RECEIVER_BILL_TO", "NAME")
    get_value_and_warn(receiver_name_info, "receiver.name", field_type="receiver field")

    receiver_billing_address_info = find_grouped_field(all_summary_fields, "RECEIVER_BILL_TO", "ADDRESS")
    get_value_and_warn(receiver_billing_address_info, "receiver.billing_address", field_type="receiver field")

    receiver_shipping_address_info = find_grouped_field(all_summary_fields, "RECEIVER_SHIP_TO", "ADDRESS")
    get_value_and_warn(receiver_shipping_address_info, "receiver.shipping_address", field_type="receiver field")

    receiver_data = {
        "name": receiver_name_info,
        "billing_address": receiver_billing_address_info,
        "shipping_address": receiver_shipping_address_info,
    }

    # --- Process Line Items ---
    line_items_data = []
    # Loop through documents to find the one with line items
    for doc in data:
        line_item_groups = doc.get("LineItemGroups", [])
        if line_item_groups:
            for group in line_item_groups:
                for item in group.get("LineItems", []):
                    item_details = {}
                    for field in item.get("LineItemExpenseFields", []):
                        raw_field_type = field.get("Type", {}).get("Text", "")
                        field_key = str(raw_field_type).lower()
                        value_detection = field.get("ValueDetection", {})

                        raw_value = (value_detection.get("Text") or "").replace("\n", " ").strip()
                        get_value_and_warn(raw_value, f"line_item.{field_key}", field_type="line item field")

                        normalized_value = raw_value if raw_value else "N/A"
                        field_confidence = value_detection.get("Confidence", 0.0)

                        item_details[field_key] = {
                            "value": normalized_value,
                            "confidence": field_confidence,
                        }
                    line_items_data.append(item_details)

    # --- Assemble the final JSON object ---
    final_json = {
        "summary": summary_data,
        "vendor": vendor_data,
        "receiver": receiver_data,
        "line_items": line_items_data
    }

    logger.info("Successfully processed Textract output :) !")
    return final_json, warnings

def get_analysis(job_id, key):
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
    expense_documents: List[Dict[str, Any]] = []
    document_metadata: Optional[Dict[str, Any]] = None
    next_token: Optional[str] = None

    logger.info(f"Retrieving Textract analysis for: {key}.")

    while True:
        if next_token:
            page = textract_client.get_expense_analysis(JobId=job_id, NextToken=next_token)
        else:
            page = textract_client.get_expense_analysis(JobId=job_id)

        document_metadata = page.get("DocumentMetadata") or document_metadata
        expense_documents.extend(page.get("ExpenseDocuments", []))
        next_token = page.get("NextToken")
        if not next_token:
            break

    try:
        processed_output, warnings = process_output(expense_documents)
    except Exception as e:
        logger.warning(f"Processing failed :(. Falling back to raw Textract output: {e}")
        return {
            "JobId": job_id,
            "JobStatus": "SUCCEEDED_WITH_WARNINGS",
            "DocumentMetadata": document_metadata,
            "Warnings": [f"Processing of Textract analysis failed: {e}"],
            "ExpenseDocuments": expense_documents,
        }

    return {
        "JobId": job_id,
        "JobStatus": "SUCCEEDED",
        "DocumentMetadata": document_metadata,
        "Warnings": warnings,
        "ExpenseDocuments": processed_output,
    }

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
    setup_structured_logging(str(context.aws_request_id), "")
    logger.info("Lambda function invoked by SNS.")

    sns_message = event["Records"]["Sns"]["Message"]

    try:
        message_data = json.loads(sns_message)
        logger.info("Successfully parsed SNS message as JSON")
        try:
            job_id = message_data["JobId"]
            key = message_data["DocumentLocation"]["S3ObjectName"]
            setup_structured_logging(str(context.aws_request_id), job_id=job_id)
            logger.info(f"Found job_id: {job_id}")
        except KeyError:
            logger.critical("Could not retrieve job_id")
            return {
                "statusCode": 500,
                "body": json.dumps("Error getting job_id")
            }
    except json.JSONDecodeError:
        logger.critical("SNS message is not a valid JSON string.")
        return {
            "statusCode": 500,
            "body": json.dumps("SNS message is not a valid JSON string.")
        }

    results = get_analysis(job_id=job_id, key=key)
    _write_to_s3(results.get("ExpenseDocuments"), output_key=job_id+"/result.json")

    return {
        "statusCode": 200,
        "body": json.dumps("SNS message processed successfully!")
    }
