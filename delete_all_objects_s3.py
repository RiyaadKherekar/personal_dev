import boto3

bucket_name = 'ct-ire-edp-prd-s3-logs'
s3 = boto3.client('s3')

def delete_all_objects(bucket):
    print(f"Starting deletion from bucket: {bucket}")
    paginator = s3.get_paginator('list_object_versions')
    to_delete = []

    for page in paginator.paginate(Bucket=bucket):
        versions = page.get('Versions', [])
        delete_markers = page.get('DeleteMarkers', [])
        all_versions = versions + delete_markers

        for obj in all_versions:
            key = obj['Key']
            version_id = obj['VersionId']
            print(f"Deleting: {key} (version: {version_id})")
            to_delete.append({'Key': key, 'VersionId': version_id})

        if len(to_delete) >= 1000:
            _delete_batch(bucket, to_delete)
            to_delete = []

    if to_delete:
        _delete_batch(bucket, to_delete)

    print("Deletion complete.")

def _delete_batch(bucket, objects):
    response = s3.delete_objects(
        Bucket=bucket,
        Delete={'Objects': objects}
    )
    deleted = response.get('Deleted', [])
    for obj in deleted:
        print(f"Confirmed deleted: {obj['Key']} (version: {obj.get('VersionId', 'N/A')})")

delete_all_objects(bucket_name)
