"""
Knowledge Base to Staging Sync
Copies new and modified files from knowledgebase to staging bucket
"""

from __future__ import annotations

import os
import logging
import boto3
import botocore.exceptions
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param, ParamsDict

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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
# MINIO FUNCTIONS
# ============================================================================

def init_s3_client(minio_endpoint, minio_access_key, minio_secret_key, minio_secure):
    """Initialize boto3 S3 client for MinIO."""
    try:
        endpoint_url = f"http{'s' if minio_secure else ''}://{minio_endpoint}"
        
        # Clear proxy settings
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("http_proxy", None)
        os.environ.pop("HTTPS_PROXY", None)
        os.environ.pop("https_proxy", None)
        
        client = boto3.client(
            's3',
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            endpoint_url=endpoint_url,
            use_ssl=minio_secure
        )
        
        logger.info(f"✅ S3 client initialized for endpoint: {endpoint_url}")
        return client
        
    except Exception as e:
        logger.error(f"❌ S3 client initialization failed: {e}")
        return None


def scan_minio_bucket(client, bucket: str) -> dict:
    """Scan MinIO bucket and return dict of files with their ETags."""
    try:
        files_dict = {}
        
        response = client.list_objects_v2(Bucket=bucket)
        
        if 'Contents' not in response:
            logger.info(f"🔭 No files found in bucket: {bucket}")
            return {}
        
        for obj in response['Contents']:
            object_key = obj['Key']
            etag = obj['ETag'].strip('"')
            size_mb = obj['Size'] / (1024 * 1024)
            
            files_dict[object_key] = {
                'etag': etag,
                'size': obj['Size'],
                'size_mb': size_mb,
                'last_modified': obj['LastModified'].isoformat(),
                'filename': os.path.basename(object_key)
            }
        
        logger.info(f"📊 Scanned {bucket}: found {len(files_dict)} file(s)")
        return files_dict
        
    except botocore.exceptions.ClientError as e:
        logger.error(f"❌ Error scanning MinIO bucket {bucket}: {e}")
        return {}
    except Exception as e:
        logger.error(f"❌ Unexpected error scanning bucket {bucket}: {e}")
        return {}


def copy_file_in_minio(client, source_bucket: str, dest_bucket: str, object_key: str) -> bool:
    """Copy file from one MinIO bucket to another."""
    try:
        copy_source = {'Bucket': source_bucket, 'Key': object_key}
        client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=object_key)
        logger.info(f"✅ Copied: {object_key} ({source_bucket} → {dest_bucket})")
        return True
    except Exception as e:
        logger.error(f"❌ Error copying {object_key}: {e}")
        return False


# ============================================================================
# MAIN SYNC FUNCTION
# ============================================================================

def sync_knowledgebase_to_staging(**context):
    """
    Sync files from knowledgebase to staging
    - Copy NEW files (not in staging)
    - Copy MODIFIED files (ETag changed)
    - NEVER delete from staging
    """
    params = context['params']
    
    logger.info("=" * 70)
    logger.info("🚀 DAG: KNOWLEDGEBASE → STAGING SYNC")
    logger.info("=" * 70)
    
    # Initialize S3 client
    s3_client = init_s3_client(
        params['minio_endpoint'],
        params['minio_access_key'],
        params['minio_secret_key'],
        params['minio_secure']
    )
    
    if not s3_client:
        raise ValueError("❌ Failed to initialize S3 client")
    
    # Scan both buckets
    logger.info(f"📂 Scanning source bucket: {params['minio_source_bucket']}")
    knowledgebase_files = scan_minio_bucket(s3_client, params['minio_source_bucket'])
    
    logger.info(f"📂 Scanning staging bucket: {params['minio_staging_bucket']}")
    staging_files = scan_minio_bucket(s3_client, params['minio_staging_bucket'])
    
    # Calculate what needs to be copied
    to_copy = []
    
    for file_key, file_info in knowledgebase_files.items():
        if file_key not in staging_files:
            # New file
            to_copy.append({
                'key': file_key,
                'reason': 'new',
                'info': file_info
            })
            logger.info(f"🆕 NEW file detected: {file_key}")
        elif staging_files[file_key]['etag'] != file_info['etag']:
            # Modified file
            to_copy.append({
                'key': file_key,
                'reason': 'modified',
                'info': file_info
            })
            logger.info(f"🔄 MODIFIED file detected: {file_key}")
    
    # Summary
    logger.info("=" * 70)
    logger.info(f"📋 SYNC PLAN:")
    logger.info(f"   📁 Total in source bucket: {len(knowledgebase_files)}")
    logger.info(f"   📁 Total in staging: {len(staging_files)}")
    logger.info(f"   ➕ New files to copy: {sum(1 for f in to_copy if f['reason'] == 'new')}")
    logger.info(f"   🔄 Modified files to copy: {sum(1 for f in to_copy if f['reason'] == 'modified')}")
    logger.info(f"   ⭐️ Unchanged files: {len(knowledgebase_files) - len(to_copy)}")
    logger.info("=" * 70)
    
    if not to_copy:
        logger.info("✅ No new or modified files - staging is up to date!")
        return
    
    # Copy files
    copied_count = 0
    failed_count = 0
    
    logger.info(f"🚀 Starting copy operation for {len(to_copy)} file(s)...")
    
    for item in to_copy:
        if copy_file_in_minio(
            s3_client,
            params['minio_source_bucket'],
            params['minio_staging_bucket'],
            item['key']
        ):
            copied_count += 1
        else:
            failed_count += 1
    
    # Final summary
    logger.info("=" * 70)
    logger.info(f"✅ COMPLETE")
    logger.info(f"📊 Results: {copied_count} copied, {failed_count} failed")
    logger.info("=" * 70)


# ============================================================================
# AIRFLOW DAG DEFINITION
# ============================================================================

dag = DAG(
    dag_id='knowledgebase_to_staging_sync',
    default_args=default_args,
    description='Copy new/modified files from knowledgebase to staging',
    schedule=None,
    catchup=False,
    tags=['minio', 'knowledgebase', 'staging'],
    params=ParamsDict(
        {
            # MinIO Configuration
            "minio_endpoint": Param(
                "",
                type="string",
                description="MinIO endpoint (e.g., 10.96.1.221:9000)",
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
            "minio_source_bucket": Param(
                "knowledgebase",
                type="string",
                description="Source bucket (knowledgebase)",
            ),
            "minio_staging_bucket": Param(
                "staging",
                type="string",
                description="Staging bucket (staging)",
            ),
            "minio_secure": Param(
                False,
                type="boolean",
                description="Use HTTPS for MinIO connection",
            ),
        }
    ),
    render_template_as_native_obj=True,
    access_control={"All": {"DAGs": {"can_read", "can_edit", "can_delete"}}},
)

# Single task
sync_task = PythonOperator(
    task_id='sync_knowledgebase_to_staging',
    python_callable=sync_knowledgebase_to_staging,
    provide_context=True,
    dag=dag,
)


sync_task