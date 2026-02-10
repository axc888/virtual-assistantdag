"""
 Staging to OpenWebUI Knowledge Base Sync
Intelligently syncs staging bucket to OpenWebUI with precise delta tracking
- Tracks all files synced to OpenWebUI in Airflow Variable
- Only adds NEW files
- Only updates MODIFIED files  
- Only deletes files that were REMOVED from staging
"""

from __future__ import annotations

import os
import time
import requests
import logging
import tempfile
import boto3
import botocore.exceptions
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict
from airflow.models import Variable

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Airflow Variable for tracking synced files
SYNC_TRACKING_VAR = "staging_openwebui_file_mapping"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================================
# TRACKING FUNCTIONS
# ============================================================================

def get_sync_tracking() -> dict:
    """
    Get tracking state: maps staging file paths to OpenWebUI file info
    Structure: {
        "staging_file_path": {
            "etag": "abc123",
            "openwebui_file_id": "uuid",
            "filename": "file.pdf",
            "synced_at": "2025-10-30T10:00:00"
        }
    }
    """
    try:
        tracking = Variable.get(SYNC_TRACKING_VAR, default_var={}, deserialize_json=True)
        if not isinstance(tracking, dict):
            logger.warning("⚠️ Tracking variable is not a dict, resetting to empty dict")
            return {}
        logger.info(f"📊 Loaded tracking: {len(tracking)} files currently tracked")
        return tracking
    except Exception as e:
        logger.error(f"❌ Error loading tracking: {e}", exc_info=True)
        return {}


def update_sync_tracking(tracking: dict) -> bool:
    """Update the tracking variable with error handling."""
    try:
        Variable.set(SYNC_TRACKING_VAR, tracking, serialize_json=True)
        logger.info(f"✅ Updated tracking: {len(tracking)} files tracked")
        return True
    except Exception as e:
        logger.error(f"❌ Error updating tracking: {e}", exc_info=True)
        return False


# ============================================================================
# MINIO FUNCTIONS
# ============================================================================

def init_s3_client(minio_endpoint: str, minio_access_key: str, 
                   minio_secret_key: str, minio_secure: bool) -> Optional[boto3.client]:
    """Initialize boto3 S3 client for MinIO with validation."""
    try:
        endpoint_url = f"http{'s' if minio_secure else ''}://{minio_endpoint}"
        
        # Clear proxy environment variables
        for proxy_var in ["HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"]:
            os.environ.pop(proxy_var, None)
        
        client = boto3.client(
            's3',
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            endpoint_url=endpoint_url,
            use_ssl=minio_secure
        )
        
        # Test connection
        client.list_buckets()
        logger.info(f"✅ S3 client initialized and validated for: {endpoint_url}")
        return client
        
    except botocore.exceptions.ClientError as e:
        logger.error(f"❌ S3 client authentication failed: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ S3 client initialization failed: {e}", exc_info=True)
        return None


def scan_minio_bucket(client, bucket: str) -> dict:
    """Scan MinIO bucket and return dict of files with their ETags."""
    try:
        files_dict = {}
        
        # Handle pagination for large buckets
        paginator = client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=bucket)
        
        for page in page_iterator:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                object_key = obj['Key']
                etag = obj['ETag'].strip('"')
                size_mb = obj['Size'] / (1024 * 1024)
                
                files_dict[object_key] = {
                    'etag': etag,
                    'size': obj['Size'],
                    'size_mb': round(size_mb, 2),
                    'last_modified': obj['LastModified'].isoformat(),
                    'filename': os.path.basename(object_key)
                }
        
        logger.info(f"📊 Scanned {bucket}: found {len(files_dict)} file(s)")
        
        # Show file details
        if files_dict:
            logger.info("📁 Files in staging bucket:")
            for path, info in files_dict.items():
                logger.info(f"   • {info['filename']} ({info['size_mb']} MB) - ETag: {info['etag'][:8]}...")
        
        return files_dict
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        logger.error(f"❌ Error scanning MinIO bucket {bucket} (Code: {error_code}): {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Unexpected error scanning bucket {bucket}: {e}", exc_info=True)
        return {}


def download_from_minio(client, object_key: str, bucket: str) -> Optional[str]:
    """Download file from MinIO to temporary location and return path."""
    temp_path = None
    try:
        temp_dir = tempfile.mkdtemp()
        filename = os.path.basename(object_key)
        temp_path = os.path.join(temp_dir, filename)
        
        logger.info(f"⬇️ Downloading from MinIO: {object_key}")
        client.download_file(bucket, object_key, temp_path)
        
        # Verify file was downloaded
        if not os.path.exists(temp_path):
            logger.error(f"❌ Downloaded file not found at: {temp_path}")
            return None
            
        file_size = os.path.getsize(temp_path)
        logger.info(f"✅ Downloaded: {filename} ({file_size / 1024 / 1024:.2f} MB) to {temp_path}")
        return temp_path
        
    except botocore.exceptions.ClientError as e:
        logger.error(f"❌ MinIO download failed for {object_key}: {e}")
        if temp_path and os.path.exists(temp_path):
            cleanup_temp_file(temp_path)
        return None
    except Exception as e:
        logger.error(f"❌ Download error for {object_key}: {e}", exc_info=True)
        if temp_path and os.path.exists(temp_path):
            cleanup_temp_file(temp_path)
        return None


# ============================================================================
# OPENWEBUI FUNCTIONS
# ============================================================================

def test_openwebui_connection(webui_url: str, openwebui_api_key: str, verify_ssl: bool) -> bool:
    """Test OpenWebUI connection and API key validity."""
    try:
        # Try to get models endpoint (usually public)
        url = f"{webui_url}/api/v1/models"
        headers = {"Authorization": f"Bearer {openwebui_api_key}"}
        
        response = requests.get(url, headers=headers, verify=verify_ssl, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"✅ OpenWebUI connection successful: {webui_url}")
            return True
        elif response.status_code == 401:
            logger.error(f"❌ OpenWebUI API key is invalid (401 Unauthorized)")
            return False
        else:
            logger.warning(f"⚠️ OpenWebUI connection returned status {response.status_code}")
            return True  # Proceed anyway
            
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Cannot connect to OpenWebUI at {webui_url}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Error testing OpenWebUI connection: {e}", exc_info=True)
        return False


def upload_file_to_webui(file_path: str, webui_url: str, openwebui_api_key: str, 
                         verify_ssl: bool, max_retries: int = 3) -> Optional[dict]:
    """Upload file to OpenWebUI with retry logic."""
    url = f"{webui_url}/api/v1/files/"
    headers = {
        "Authorization": f"Bearer {openwebui_api_key}",
        "Accept": "application/json"
    }

    filename = os.path.basename(file_path)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"⬆️ Uploading to OpenWebUI (attempt {attempt}/{max_retries}): {filename}")
            
            with open(file_path, "rb") as f:
                files = {"file": (filename, f)}
                response = requests.post(
                    url, 
                    headers=headers, 
                    files=files, 
                    timeout=300, 
                    verify=verify_ssl
                )

            logger.info(f"📡 Upload response status: {response.status_code}")
            
            if response.status_code == 200:
                uploaded_file = response.json()
                file_id = uploaded_file.get('id', 'Unknown')
                logger.info(f"✅ File uploaded successfully: {filename} → ID: {file_id}")
                logger.debug(f"🔍 Upload response: {json.dumps(uploaded_file, indent=2)}")
                return uploaded_file
            
            elif response.status_code in [401, 403]:
                logger.error(f"❌ Authentication failed: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None  # Don't retry auth failures
                
            else:
                logger.error(f"❌ Upload failed with status {response.status_code}: {response.text}")
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"⏳ Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"⏰ Upload timeout for {filename}")
            if attempt < max_retries:
                logger.info(f"⏳ Retrying...")
                continue
            return None
            
        except Exception as e:
            logger.error(f"❌ Upload error: {e}", exc_info=True)
            if attempt < max_retries:
                time.sleep(2)
                continue
            return None
    
    return None


def wait_until_processed(file_id: str, file_size_mb: float, webui_url: str, 
                        openwebui_api_key: str, verify_ssl: bool, 
                        max_wait_time: int, poll_interval: int, 
                        max_poll_interval: int) -> bool:
    """
    Poll file status until it's processed or timeout.
    Handles None status, better logging, graceful degradation
    """
    url = f"{webui_url}/api/v1/files/{file_id}"
    headers = {"Authorization": f"Bearer {openwebui_api_key}"}

    start_time = time.time()
    attempt = 0
    current_poll_interval = poll_interval
    
    success_statuses = ["success", "completed", "ready", "processed"]
    failed_statuses = ["failed", "error", "cancelled"]
    
    logger.info(f"⏱️ Starting status monitoring for file: {file_id}")
    logger.info(f"📊 File size: {file_size_mb:.2f} MB, Max wait: {max_wait_time}s")
    
    while time.time() - start_time < max_wait_time:
        attempt += 1
        elapsed = int(time.time() - start_time)
        
        try:
            response = requests.get(url, headers=headers, verify=verify_ssl, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"⚠️ Status check returned {response.status_code}: {response.text}")
                time.sleep(current_poll_interval)
                continue
            
            file_info = response.json()
            
            # DEBUG: Log full response on first few attempts
            if attempt <= 3:
                logger.info(f"🔍 DEBUG - Full API response #{attempt}:")
                logger.info(f"{json.dumps(file_info, indent=2)}")
            
            # Try multiple paths to get status
            status = None
            if isinstance(file_info, dict):
                # Try different possible locations
                status = (
                    file_info.get("data", {}).get("status") or
                    file_info.get("status") or
                    file_info.get("meta", {}).get("status")
                )
            
            logger.info(f"📊 Status check #{attempt} (elapsed: {elapsed}s): status='{status}'")
            
            # Handle None status (OpenWebUI might not use status field)
            if status is None:
                logger.warning(f"⚠️ Status field is None/missing")
                
                # After a few attempts with None status, check if file actually exists
                if attempt >= 3:
                    logger.info(f"🔍 Checking if file exists despite None status...")
                    
                    # Check if we can get the file info at all
                    if file_info and isinstance(file_info, dict):
                        file_exists = file_info.get('id') == file_id
                        
                        if file_exists:
                            logger.info(f"✅ File {file_id} exists in system (status=None might be normal)")
                            logger.info(f"⏱️ Waiting brief period for metadata extraction...")
                            
                            # Wait based on file size for metadata processing
                            metadata_wait = min(30 + (file_size_mb * 10), 180)
                            logger.info(f"⏱️ Waiting {metadata_wait}s for processing...")
                            time.sleep(metadata_wait)
                            
                            logger.info(f"✅ Assuming file {file_id} is ready (OpenWebUI may not set status)")
                            return True
                
                # Continue polling
                time.sleep(current_poll_interval)
                current_poll_interval = min(current_poll_interval * 1.5, max_poll_interval)
                continue
            
            # Handle actual status values
            status_lower = str(status).lower()
            
            if status_lower in success_statuses:
                logger.info(f"✅ File processing successful: {status}")
                
                # Additional wait for metadata extraction
                metadata_wait = min(60 + (file_size_mb * 30), 1800)
                logger.info(f"⏱️ Waiting {metadata_wait}s for metadata extraction...")
                time.sleep(metadata_wait)
                
                return True
                
            elif status_lower in failed_statuses:
                logger.error(f"❌ File processing failed with status: {status}")
                return False
                
            elif status_lower == "pending" or status_lower == "processing":
                logger.info(f"⏳ Still processing... (status: {status})")
                
            else:
                logger.warning(f"⚠️ Unknown status: {status}")
                # Continue polling for unknown status
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Network error checking status: {e}")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON response: {e}")
        except Exception as e:
            logger.error(f"❌ Error checking status: {e}", exc_info=True)

        # Wait before next poll
        time.sleep(current_poll_interval)
        current_poll_interval = min(current_poll_interval * 1.5, max_poll_interval)

    logger.warning(f"⏰ Timeout: File {file_id} not confirmed processed within {max_wait_time}s")
    logger.warning(f"⚠️ Proceeding anyway - file may still be processing")
    return True  # Proceed anyway instead of failing


def add_file_to_knowledge(knowledge_id: str, file_id: str, webui_url: str, 
                         openwebui_api_key: str, verify_ssl: bool, 
                         max_retries: int = 3) -> Optional[dict]:
    """Add file to knowledge base with retry logic."""
    url = f'{webui_url}/api/v1/knowledge/{knowledge_id}/file/add'
    headers = {
        'Authorization': f'Bearer {openwebui_api_key}',
        'Content-Type': 'application/json'
    }
    data = {'file_id': file_id}
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"📚 Adding file to knowledge base (attempt {attempt}/{max_retries})...")
            response = requests.post(url, headers=headers, json=data, verify=verify_ssl, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"✅ File added to knowledge base successfully")
                return result
            elif response.status_code == 409:
                logger.warning(f"⚠️ File already in knowledge base (409 Conflict)")
                return {"status": "already_exists"}
            elif response.status_code in [401, 403]:
                logger.error(f"❌ Authentication failed: {response.status_code}")
                return None  # Don't retry auth failures
            else:
                logger.error(f"❌ Failed to add to KB ({response.status_code}): {response.text}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return None
                
        except Exception as e:
            logger.error(f"❌ Error adding to knowledge base: {e}", exc_info=True)
            if attempt < max_retries:
                time.sleep(2)
                continue
            return None
    
    return None


def remove_file_from_knowledge(knowledge_id: str, file_id: str, webui_url: str, 
                               openwebui_api_key: str, verify_ssl: bool,
                               max_retries: int = 3) -> bool:
    """Remove file from knowledge base with retry logic."""
    url = f'{webui_url}/api/v1/knowledge/{knowledge_id}/file/remove'
    headers = {
        'Authorization': f'Bearer {openwebui_api_key}',
        'Content-Type': 'application/json'
    }
    data = {'file_id': file_id}
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🗑️ Removing file from knowledge base (attempt {attempt}/{max_retries})...")
            response = requests.post(url, headers=headers, json=data, verify=verify_ssl, timeout=30)
            
            if response.status_code == 200:
                logger.info(f"✅ File removed from knowledge base: {file_id}")
                return True
            elif response.status_code == 404:
                logger.warning(f"⚠️ File not found in knowledge base (404)")
                return True  # Already removed
            else:
                logger.error(f"❌ Failed to remove from KB ({response.status_code}): {response.text}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return False
                
        except Exception as e:
            logger.error(f"❌ Error removing from knowledge base: {e}", exc_info=True)
            if attempt < max_retries:
                time.sleep(2)
                continue
            return False
    
    return False


def cleanup_temp_file(temp_file_path: str):
    """Cleanup temporary file and directory."""
    try:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logger.debug(f"🧹 Cleaned up temp file: {temp_file_path}")
            
            temp_dir = os.path.dirname(temp_file_path)
            if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                os.rmdir(temp_dir)
                logger.debug(f"🧹 Cleaned up temp directory: {temp_dir}")
    except Exception as e:
        logger.warning(f"⚠️ Could not cleanup temp file {temp_file_path}: {e}")


# ============================================================================
# DELTA CALCULATION
# ============================================================================

def calculate_delta(staging_files: dict, tracking: dict) -> Tuple[List, List, List, List]:
    """
    Calculate precise delta by comparing staging bucket with tracking variable.
    Returns: (to_add, to_update, to_delete, unchanged)
    """
    to_add = []
    to_update = []
    to_delete = []
    unchanged = []
    
    logger.info("🔍 Calculating delta between staging and tracking...")
    
    # Check each file in staging
    for file_path, file_info in staging_files.items():
        if file_path in tracking:
            # File was previously synced
            tracked_info = tracking[file_path]
            
            if tracked_info.get('etag') == file_info['etag']:
                # Unchanged - skip
                unchanged.append(file_path)
                logger.debug(f"⏭️ UNCHANGED: {file_path} (ETag match)")
            else:
                # ETag changed - file was modified
                to_update.append({
                    'staging_path': file_path,
                    'file_info': file_info,
                    'old_openwebui_file_id': tracked_info.get('openwebui_file_id'),
                    'old_etag': tracked_info.get('etag')
                })
                logger.info(f"🔄 MODIFIED: {file_path}")
                logger.info(f"   Old ETag: {tracked_info.get('etag', 'N/A')[:12]}...")
                logger.info(f"   New ETag: {file_info['etag'][:12]}...")
        else:
            # New file - not in tracking
            to_add.append({
                'staging_path': file_path,
                'file_info': file_info
            })
            logger.info(f"➕ NEW: {file_path}")
    
    # Find deleted files: in tracking but not in staging
    staging_paths = set(staging_files.keys())
    for tracked_path, tracked_info in tracking.items():
        if tracked_path not in staging_paths:
            to_delete.append({
                'staging_path': tracked_path,
                'filename': tracked_info.get('filename'),
                'openwebui_file_id': tracked_info.get('openwebui_file_id')
            })
            logger.info(f"🗑️ DELETED FROM STAGING: {tracked_path}")
    
    return to_add, to_update, to_delete, unchanged


# ============================================================================
# FILE PROCESSING
# ============================================================================

def process_addition(item: dict, s3_client, params: dict, tracking: dict) -> bool:
    """Add a new file to OpenWebUI and track it."""
    staging_path = item['staging_path']
    file_info = item['file_info']
    filename = file_info['filename']
    
    temp_path = None
    
    try:
        logger.info("=" * 70)
        logger.info(f"➕ ADDING: {filename}")
        logger.info(f"   Path: {staging_path}")
        logger.info(f"   Size: {file_info['size_mb']} MB")
        logger.info("=" * 70)
        
        # Download from staging
        temp_path = download_from_minio(s3_client, staging_path, params['minio_staging_bucket'])
        if not temp_path:
            logger.error(f"❌ Failed to download {staging_path}")
            return False
        
        # Upload to OpenWebUI
        uploaded_file = upload_file_to_webui(
            temp_path,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl'],
            max_retries=3
        )
        
        if not uploaded_file or "id" not in uploaded_file:
            logger.error(f"❌ Failed to upload {filename} to OpenWebUI")
            cleanup_temp_file(temp_path)
            return False
        
        file_id = uploaded_file["id"]
        logger.info(f"✅ Upload successful, file ID: {file_id}")
        
        # Wait for processing
        logger.info(f"⏱️ Waiting for file processing...")
        processing_success = wait_until_processed(
            file_id,
            file_info['size_mb'],
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl'],
            params['max_wait_time'],
            params['poll_interval'],
            params['max_poll_interval']
        )
        
        if not processing_success:
            logger.warning(f"⚠️ Processing timeout/failure for {filename}, proceeding anyway")
        
        # Add to knowledge base
        logger.info(f"📚 Adding to knowledge base...")
        kb_result = add_file_to_knowledge(
            params['knowledge_id'],
            file_id,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl'],
            max_retries=3
        )
        
        # Track the file even if KB add fails (file is still uploaded)
        tracking[staging_path] = {
            'etag': file_info['etag'],
            'openwebui_file_id': file_id,
            'filename': filename,
            'size_mb': file_info['size_mb'],
            'synced_at': datetime.now().isoformat(),
            'kb_added': kb_result is not None
        }
        
        logger.info(f"✅ Successfully added and tracked: {filename}")
        cleanup_temp_file(temp_path)
        return True
        
    except Exception as e:
        logger.error(f"❌ Error adding {staging_path}: {e}", exc_info=True)
        if temp_path:
            cleanup_temp_file(temp_path)
        return False


def process_update(item: dict, s3_client, params: dict, tracking: dict) -> bool:
    """Update an existing file in OpenWebUI."""
    staging_path = item['staging_path']
    old_file_id = item.get('old_openwebui_file_id')
    old_etag = item.get('old_etag', 'N/A')
    new_etag = item['file_info']['etag']
    
    try:
        logger.info("=" * 70)
        logger.info(f"🔄 UPDATING: {item['file_info']['filename']}")
        logger.info(f"   Path: {staging_path}")
        logger.info(f"   Old ETag: {old_etag[:12]}...")
        logger.info(f"   New ETag: {new_etag[:12]}...")
        logger.info("=" * 70)
        
        # Remove old version from knowledge base
        if old_file_id:
            logger.info(f"🗑️ Removing old version (ID: {old_file_id})...")
            remove_file_from_knowledge(
                params['knowledge_id'],
                old_file_id,
                params['webui_url'],
                params['openwebui_api_key'],
                params['verify_ssl'],
                max_retries=2
            )
        
        # Add new version (reuse addition logic)
        return process_addition(item, s3_client, params, tracking)
        
    except Exception as e:
        logger.error(f"❌ Error updating {staging_path}: {e}", exc_info=True)
        return False


def process_deletion(item: dict, params: dict, tracking: dict) -> bool:
    """Delete a file from OpenWebUI."""
    staging_path = item['staging_path']
    filename = item['filename']
    file_id = item['openwebui_file_id']
    
    try:
        logger.info("=" * 70)
        logger.info(f"🗑️ DELETING: {filename}")
        logger.info(f"   Was at: {staging_path}")
        logger.info(f"   File ID: {file_id}")
        logger.info("=" * 70)
        
        if not file_id:
            logger.warning(f"⚠️ No file_id for {staging_path} - cannot delete from OpenWebUI")
            # Still remove from tracking
            if staging_path in tracking:
                del tracking[staging_path]
                logger.info(f"✅ Removed from tracking: {staging_path}")
            return False
        
        # Remove from knowledge base
        success = remove_file_from_knowledge(
            params['knowledge_id'],
            file_id,
            params['webui_url'],
            params['openwebui_api_key'],
            params['verify_ssl'],
            max_retries=3
        )
        
        if success:
            # Remove from tracking
            if staging_path in tracking:
                del tracking[staging_path]
            logger.info(f"✅ Deleted and removed from tracking: {filename}")
            return True
        else:
            logger.error(f"❌ Failed to delete {filename}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Error deleting {staging_path}: {e}", exc_info=True)
        return False


# ============================================================================
# MAIN SYNC FUNCTION
# ============================================================================

def sync_staging_to_openwebui(**context):
    """
    Intelligent delta sync from staging to OpenWebUI.
    Uses tracking variable to identify exact changes.
    Better error handling, debugging, and status logic.
    """
    params = context['params']
    
    logger.info("=" * 80)
    logger.info("🚀 STAGING → OPENWEBUI INTELLIGENT DELTA SYNC)")
    logger.info("=" * 80)
    logger.info(f"🌐 WebUI URL: {params['webui_url']}")
    logger.info(f"🧠 Knowledge ID: {params['knowledge_id']}")
    logger.info(f"📦 MinIO Endpoint: {params['minio_endpoint']}")
    logger.info(f"🗂️ Staging Bucket: {params['minio_staging_bucket']}")
    logger.info(f"🔒 SSL Verification: {params['verify_ssl']}")
    logger.info("=" * 80)
    
    # Validate API key
    if not params['openwebui_api_key'] or params['openwebui_api_key'] == 'sk-xxxxx':
        raise ValueError("❌ Valid OpenWebUI API key is required in params")
    
    # Test OpenWebUI connection
    logger.info("🔍 Testing OpenWebUI connection...")
    if not test_openwebui_connection(params['webui_url'], params['openwebui_api_key'], params['verify_ssl']):
        raise ValueError("❌ Cannot connect to OpenWebUI - check URL and API key")
    
    # Initialize S3 client
    logger.info("🔧 Initializing MinIO S3 client...")
    s3_client = init_s3_client(
        params['minio_endpoint'],
        params['minio_access_key'],
        params['minio_secret_key'],
        params['minio_secure']
    )
    
    if not s3_client:
        raise ValueError("❌ Failed to initialize S3 client - check MinIO credentials")
    
    # Get current state
    logger.info(f"📂 Scanning staging bucket: {params['minio_staging_bucket']}")
    staging_files = scan_minio_bucket(s3_client, params['minio_staging_bucket'])
    
    if not staging_files:
        logger.warning("⚠️ No files found in staging bucket")
    
    logger.info(f"📋 Loading tracking variable: {SYNC_TRACKING_VAR}")
    tracking = get_sync_tracking()
    
    # Calculate delta
    logger.info("🔍 Calculating delta (what changed)...")
    to_add, to_update, to_delete, unchanged = calculate_delta(staging_files, tracking)
    
    # Report delta
    logger.info("=" * 80)
    logger.info(f"📊 INTELLIGENT DELTA ANALYSIS:")
    logger.info(f"   ➕ {len(to_add)} NEW files to add")
    logger.info(f"   🔄 {len(to_update)} MODIFIED files to update")
    logger.info(f"   🗑️ {len(to_delete)} DELETED files to remove")
    logger.info(f"   ⏭️ {len(unchanged)} UNCHANGED files (skipped)")
    logger.info(f"   📊 Total in staging: {len(staging_files)}")
    logger.info(f"   📊 Total tracked: {len(tracking)}")
    logger.info("=" * 80)
    
    # Show details of what will be done
    if to_add:
        logger.info("➕ NEW FILES TO ADD:")
        for item in to_add:
            info = item['file_info']
            logger.info(f"   • {info['filename']} ({info['size_mb']} MB)")
        logger.info("-" * 80)
    
    if to_update:
        logger.info("🔄 MODIFIED FILES TO UPDATE:")
        for item in to_update:
            info = item['file_info']
            logger.info(f"   • {info['filename']} ({info['size_mb']} MB)")
        logger.info("-" * 80)
    
    if to_delete:
        logger.info("🗑️ FILES TO DELETE:")
        for item in to_delete:
            logger.info(f"   • {item['filename']} (path: {item['staging_path']})")
        logger.info("-" * 80)
    
    # If nothing to do, exit
    if not to_add and not to_update and not to_delete:
        logger.info("✅ No changes detected - staging and OpenWebUI are in sync!")
        logger.info("=" * 80)
        return
    
    # Execute operations
    results = {
        'added': 0,
        'updated': 0,
        'deleted': 0,
        'failed': 0,
        'start_time': time.time()
    }
    
    # Process ADDITIONS
    if to_add:
        logger.info(f"➕ Processing {len(to_add)} additions...")
        for i, item in enumerate(to_add, 1):
            logger.info(f"Progress: {i}/{len(to_add)} additions")
            if process_addition(item, s3_client, params, tracking):
                results['added'] += 1
                # Save tracking after each successful addition
                update_sync_tracking(tracking)
            else:
                results['failed'] += 1
    
    # Process UPDATES
    if to_update:
        logger.info(f"🔄 Processing {len(to_update)} updates...")
        for i, item in enumerate(to_update, 1):
            logger.info(f"Progress: {i}/{len(to_update)} updates")
            if process_update(item, s3_client, params, tracking):
                results['updated'] += 1
                # Save tracking after each successful update
                update_sync_tracking(tracking)
            else:
                results['failed'] += 1
    
    # Process DELETIONS
    if to_delete:
        logger.info(f"🗑️ Processing {len(to_delete)} deletions...")
        for i, item in enumerate(to_delete, 1):
            logger.info(f"Progress: {i}/{len(to_delete)} deletions")
            if process_deletion(item, params, tracking):
                results['deleted'] += 1
                # Save tracking after each successful deletion
                update_sync_tracking(tracking)
            else:
                results['failed'] += 1
    
    # Final tracking update
    update_sync_tracking(tracking)
    
    # Calculate duration
    duration = time.time() - results['start_time']
    duration_mins = int(duration / 60)
    duration_secs = int(duration % 60)
    
    # Final summary
    logger.info("=" * 80)
    logger.info(f"✅ SYNC COMPLETE!")
    logger.info(f"⏱️ Duration: {duration_mins}m {duration_secs}s")
    logger.info("=" * 80)
    logger.info(f"📊 RESULTS:")
    logger.info(f"   ✅ {results['added']} files added")
    logger.info(f"   ✅ {results['updated']} files updated")
    logger.info(f"   ✅ {results['deleted']} files deleted")
    logger.info(f"   ⏭️ {len(unchanged)} files skipped (unchanged)")
    logger.info(f"   ❌ {results['failed']} operations failed")
    logger.info("-" * 80)
    logger.info(f"📋 Total files in tracking: {len(tracking)}")
    logger.info(f"📦 Total files in staging: {len(staging_files)}")
    logger.info("=" * 80)
    
    if results['failed'] > 0:
        logger.warning(f"⚠️ {results['failed']} operations failed - check logs above")
    else:
        logger.info("🎉 All operations completed successfully!")


# ============================================================================
# AIRFLOW DAG DEFINITION
# ============================================================================

dag = DAG(
    dag_id='staging_to_openwebui_sync',
    default_args=default_args,
    description='Intelligent delta sync from staging to OpenWebUI',
    schedule=None,
    catchup=False,
    tags=['minio', 'openwebui', 'knowledge-base', 'staging', 'delta'],
    params=ParamsDict(
        {
     # MinIO Configuration
            "minio_endpoint": Param(
                "",
                type="string",
                description="MinIO endpoint (e.g., 10.96.2.136:9000)",
            ),
            "minio_access_key": Param(
                "admin-io",
                type="string",
                description="MinIO access key",
            ),
            "minio_secret_key": Param(
                "MinIO$2K",
                type="string",
                description="MinIO secret key",
            ),
            "minio_staging_bucket": Param(
                "staging",
                type="string",
                description="Staging bucket (source of truth)",
            ),
            "minio_secure": Param(
                False,
                type="boolean",
                description="Use HTTPS for MinIO",
            ),
            
            # OpenWebUI Configuration
            "webui_url": Param(
                "",
                type="string",
                description="OpenWebUI URL (e.g., http://10.96.0.101:80)",
            ),
            "openwebui_api_key": Param(
                "",
                type="string",
                description="OpenWebUI API key (e.g., sk-xxxxx) - REQUIRED",
            ),
            "knowledge_id": Param(
                "",
                type="string",
                description="Knowledge base UUID (e.g., 3b389b32-df60-498a-93bf-4ecb0d6bcd69)",
            ),
            "verify_ssl": Param(
                False,
                type="boolean",
                description="Verify SSL certificates for OpenWebUI",
            ),
            
            # Timing Configuration
            "max_wait_time": Param(
                3600,
                type="integer",
                description="Max wait time for file processing (seconds)",
            ),
            "max_poll_interval": Param(
                60,
                type="integer",
                description="Max polling interval (seconds)",
            ),
            "poll_interval": Param(
                5,
                type="integer",
                description="Initial polling interval (seconds)",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)

# Main sync task
sync_task = PythonOperator(
    task_id='sync_staging_to_openwebui',
    python_callable=sync_staging_to_openwebui,
    provide_context=True,
    dag=dag,
)

sync_task
