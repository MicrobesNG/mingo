import os
import glob
import json
import logging
import subprocess
import boto3
from botocore.exceptions import ClientError
from slack_sdk.webhook import WebhookClient
import csv

logger = logging.getLogger(__name__)

class MingoUploader:
    def __init__(self, s3_bucket_fastq=None, s3_bucket_pod5=None, s3_root_folder=None,
                 slack_webhook_url=None, s3_endpoint_url=None):
        self.s3_endpoint_url = s3_endpoint_url
        if s3_endpoint_url:
            self.s3_client = boto3.client('s3', endpoint_url=s3_endpoint_url)
        else:
            self.s3_client = boto3.client('s3')
            
        self.s3_bucket_fastq = s3_bucket_fastq
        self.s3_bucket_pod5 = s3_bucket_pod5
        self.s3_root_folder = s3_root_folder.strip('/') if s3_root_folder else ""
        
        if not slack_webhook_url:
            raise ValueError("A Slack Webhook URL must be provided to MingoUploader.")
            
        self.slack_webhook_url = slack_webhook_url
        self.slack_client = WebhookClient(self.slack_webhook_url)

    def send_slack_notification(self, message: str, run_name: str = None, success: bool = True, fastq_count: int = 0, pod5_count: int = 0):
        color = "#2EB67D" if success else "#E01E5A"
        header = "📦 S3 Upload Completed Successfully" if success else "❌ S3 Upload Completed with Errors"
        
        fields = []
        if run_name:
            fields.append({"type": "mrkdwn", "text": f"*Run Name:*\n`{run_name}`"})
        if self.s3_bucket_fastq:
            fields.append({"type": "mrkdwn", "text": f"*FASTQ Bucket:*\n`{self.s3_bucket_fastq}`"})
        if self.s3_bucket_pod5:
            fields.append({"type": "mrkdwn", "text": f"*POD5 Bucket:*\n`{self.s3_bucket_pod5}`"})
        if fastq_count or pod5_count:
            fields.append({"type": "mrkdwn", "text": f"*Upload Summary:*\n⚡ `{fastq_count}` FASTQ, `{pod5_count}` POD5 orders"})

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": header,
                    "emoji": True
                }
            }
        ]
        if fields:
            blocks.append({"type": "section", "fields": fields})
        else:
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": message}})
            
        import time
        now_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": f"☁️ *MiNGo Uploader* • {now_str}"}]
        })

        try:
            response = self.slack_client.send(
                text=message,
                attachments=[{"color": color, "blocks": blocks}]
            )
            if response.status_code != 200:
                logger.error(f"Failed to send Slack message. Error: {response.body}")
        except Exception as e:
            logger.error(f"Error sending Slack notification: {e}")

    def _split_bucket_prefix(self, bucket_str, destination_key):
        parts = bucket_str.split('/', 1)
        bucket_name = parts[0]
        if len(parts) > 1:
            destination_key = f"{parts[1]}/{destination_key}"
        return bucket_name, destination_key

    def _build_public_url(self, bucket_name, destination_key):
        if self.s3_endpoint_url:
            endpoint = self.s3_endpoint_url.rstrip('/')
            return f"{endpoint}/{bucket_name}/{destination_key}"
        else:
            return f"https://{bucket_name}.s3.amazonaws.com/{destination_key}"

    def file_exists_on_s3(self, bucket_str, key, expected_size=None, tolerance=0.01):
        """Checks if a file exists on S3. If expected_size is provided, does a fuzzy size match."""
        bucket_name, key = self._split_bucket_prefix(bucket_str, key)
        try:
            response = self.s3_client.head_object(Bucket=bucket_name, Key=key)
            if expected_size is not None:
                remote_size = response['ContentLength']
                if expected_size == remote_size:
                    return True
                
                # Fuzzy size check for resumed/re-uploaded files
                size_diff = abs(expected_size - remote_size)
                if expected_size > 0:
                    diff_percentage = size_diff / expected_size
                    if diff_percentage <= tolerance:
                        logger.debug(f"File {key} fuzzy size match (Expected: {expected_size}, Remote: {remote_size}, Diff: {diff_percentage:.2%})")
                        return True
                        
                logger.warning(f"File {key} exists but size mismatch (Expected: {expected_size}, Remote: {remote_size}). Will overwrite.")
                return False
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.error(f"Error checking S3 for {key}: {e}")
                raise

    def parse_sample_sheet(self, csv_path):
        """Extracts mapping of barcode -> alias and orderName."""
        samples = {}
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    barcode = row['barcode']
                    
                    # Handle Native Barcode renaming (from Upload_Script.sh logic 'NB' -> 'BC')
                    if barcode.startswith('NB'):
                        barcode = barcode.replace('NB', 'BC')

                    samples[barcode] = {
                        'alias': row['alias'],
                        'order_name': row.get('cntn_cf_orderName', 'Unknown_Order')
                    }
        except Exception as e:
            logger.error(f"Error parsing sample sheet {csv_path}: {e}")
            raise
        return samples

    def upload_fastq_for_barcode(self, top_run_dir, sub_run, barcode, alias, bucket):
        run_name = os.path.basename(top_run_dir)
        fastq_dir = os.path.join(top_run_dir, "no_sample_id", sub_run, "fastq_pass", alias)
        
        if not os.path.exists(fastq_dir):
            logger.warning(f"No fastq_pass directory found for alias {alias} at {fastq_dir}")
            return None

        fastqs = glob.glob(os.path.join(fastq_dir, "*.fastq.gz"))
        if not fastqs:
            logger.warning(f"No fastq.gz files found in {fastq_dir}")
            return None

        destination_key = f"{run_name}/{alias}_{sub_run}_{barcode}.fastq.gz"
        
        # Calculate expected size to ensure robustness against mid-upload terminations
        expected_size = sum(os.path.getsize(f) for f in fastqs)
        
        if self.file_exists_on_s3(bucket, destination_key, expected_size=expected_size):
            bucket_name, dest_key = self._split_bucket_prefix(bucket, destination_key)
            logger.info(f"Skipping {dest_key} - already exists on S3 with matching size (~{expected_size/1024/1024:.2f} MB).")
            return self._build_public_url(bucket_name, dest_key)

        bucket_name, destination_key = self._split_bucket_prefix(bucket, destination_key)
        logger.info(f"Uploading {len(fastqs)} FASTQ files for {barcode} to s3://{bucket_name}/{destination_key}...")
        
        # Stream concatenation using subproccess directly into boto3 upload_fileobj
        # This avoids creating massive temporary files locally.
        try:
            cat_command = ['cat'] + fastqs
            logger.debug(f"Running concatenation: {' '.join(cat_command[:3])}... ({len(fastqs)} files total)")
            
            with subprocess.Popen(cat_command, stdout=subprocess.PIPE) as proc:
                self.s3_client.upload_fileobj(proc.stdout, bucket_name, destination_key)
                proc.communicate() # Wait for process to finish and check return code
                if proc.returncode != 0:
                    raise subprocess.CalledProcessError(proc.returncode, cat_command)
            
            logger.info(f"Successfully uploaded {destination_key}")
            return self._build_public_url(bucket_name, destination_key)
            
        except Exception as e:
            logger.error(f"Failed to upload merged FASTQs for {barcode}: {e}")
            raise

    def get_run_metadata(self, run_dir):
        """Extract run name and sub-run from the directory structure."""
        run_dir_abs = os.path.abspath(run_dir)
        # Expected: /.../no_sample_id/20260220_1403_P2S-01064-B_PBI35250_84b0eb00
        sub_run = os.path.basename(run_dir_abs)
        
        # Expected: /.../no_sample_id (Wait, Upload_Script.sh gets the run name from the top dir)
        # Assuming run_dir passed is the sub_run dir, the top run dir is two levels up:
        # TopDir (e.g. NSR...) / no_sample_id / SubRunDir (e.g. 20260...)
        no_sample_dir = os.path.dirname(run_dir_abs)
        top_run_dir = os.path.dirname(no_sample_dir)
        run_name = os.path.basename(top_run_dir)
        
        return run_name, sub_run, top_run_dir

    def upload_pod5_for_barcode(self, top_run_dir, sub_run, barcode, alias, order_name, bucket):
        run_name = os.path.basename(top_run_dir)
        pod5_dir = os.path.join(top_run_dir, "no_sample_id", sub_run, "pod5_pass", alias)
        
        if not os.path.exists(pod5_dir):
            logger.warning(f"No pod5_pass directory found for alias {alias} at {pod5_dir}")
            return []

        pod5s = glob.glob(os.path.join(pod5_dir, "*.pod5"))
        if not pod5s:
            logger.warning(f"No pod5 files found in {pod5_dir}")
            return []

        uploaded_uris = []
        # Structure: s3://<bucket>/[<root>]/<cntn_cf_orderName>/<barcode_alias>/<filename>
        for pod5_path in pod5s:
            filename = os.path.basename(pod5_path)
            prefix = f"{self.s3_root_folder}/" if self.s3_root_folder else ""
            destination_key = f"{prefix}{order_name}/{alias}/{filename}"
            
            expected_size = os.path.getsize(pod5_path)
            if self.file_exists_on_s3(bucket, destination_key, expected_size=expected_size):
                bucket_name, dest_key = self._split_bucket_prefix(bucket, destination_key)
                logger.debug(f"Skipping {dest_key} - already exists on S3 with matching size.")
                uploaded_uris.append(self._build_public_url(bucket_name, dest_key))
                continue

            bucket_name, destination_key = self._split_bucket_prefix(bucket, destination_key)
            logger.info(f"Uploading {filename} to s3://{bucket_name}/{destination_key}...")
            try:
                self.s3_client.upload_file(pod5_path, bucket_name, destination_key)
                uploaded_uris.append(self._build_public_url(bucket_name, destination_key))
            except Exception as e:
                logger.error(f"Failed to upload {pod5_path}: {e}")
                raise
                
        return uploaded_uris

    def write_manifest(self, top_run_dir, run_name, manifest_type, manifest_data, order_name=None, s3_bucket=None):
        """Writes a JSON manifest of uploaded files and optionally uploads it to S3."""
        if not manifest_data:
            return None
            
        if manifest_type == "pod5" and order_name:
            manifest_filename = f"manifest_{manifest_type}_{order_name}.json"
        else:
            manifest_filename = f"manifest_{manifest_type}_{run_name}.json"
            
        if manifest_type == "pod5" and order_name and s3_bucket:
            prefix = f"{self.s3_root_folder}/" if self.s3_root_folder else ""
            dest_key = f"{prefix}{order_name}/{manifest_filename}"
            bucket_name, dest_key = self._split_bucket_prefix(s3_bucket, dest_key)
            
            try:
                response = self.s3_client.get_object(Bucket=bucket_name, Key=dest_key)
                existing_data = json.loads(response['Body'].read().decode('utf-8'))
                
                # Merge existing URIs with new ones
                if isinstance(existing_data, list):
                    existing_uris = {item.get('s3_uri') for item in existing_data if isinstance(item, dict)}
                    for item in manifest_data:
                        if item.get('s3_uri') not in existing_uris:
                            existing_data.append(item)
                    manifest_data = existing_data
                    logger.info(f"Merged new uploads with existing manifest {dest_key}")
            except ClientError as e:
                if e.response['Error']['Code'] not in ['NoSuchKey', '404']:
                    logger.warning(f"Failed to fetch existing manifest {dest_key}, proceeding with override: {e}")
            except Exception as e:
                logger.warning(f"Failed to parse existing manifest {dest_key}, proceeding with override: {e}")
            
        manifest_path = os.path.join(top_run_dir, manifest_filename)
        
        try:
            with open(manifest_path, 'w') as f:
                json.dump(manifest_data, f, indent=4)
            logger.info(f"Wrote {manifest_type} manifest to {manifest_path}")

            if manifest_type == "pod5" and order_name and s3_bucket:
                prefix = f"{self.s3_root_folder}/" if self.s3_root_folder else ""
                dest_key = f"{prefix}{order_name}/{manifest_filename}"
                bucket_name, dest_key = self._split_bucket_prefix(s3_bucket, dest_key)
                
                logger.info(f"Uploading POD5 manifest to s3://{bucket_name}/{dest_key}...")
                self.s3_client.upload_file(manifest_path, bucket_name, dest_key)

            return manifest_path
        except Exception as e:
            logger.error(f"Failed to write manifest {manifest_path}: {e}")
            return None
