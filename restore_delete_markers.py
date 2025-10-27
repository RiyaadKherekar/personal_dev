import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client('s3')

bucket_name = "ct-ire-edp-prod-africa-everest"
source_prefix = "changeaudit"
target_prefix = "changeaudit"
excluded_country = "everest_kenya"

print("Starting scan for versioned Parquet files...")

paginator = s3.get_paginator('list_object_versions')
page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=source_prefix)

restore_tasks = []

def restore_object(version):
    key = version['Key']
    version_id = version['VersionId']

    # Restore only .parquet, non-latest, non-delete-marker
    if not key.endswith(".parquet"):
        return None
    if version.get('IsDeleteMarker', False):
        return None
    if version.get('IsLatest', False):
        return None
    if f"{source_prefix}/{excluded_country}" in key:
        print(f"Skipping key (excluded): {key}")
        return None

    new_key = key.replace(source_prefix, target_prefix, 1)

    print("\n--------------------------------------------")
    print(f"Original key     : {key}")
    print(f"Version ID       : {version_id}")
    print(f"New target key   : {new_key}")
    print(f"Copying to s3://{bucket_name}/{new_key} ...")

    try:
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': key, 'VersionId': version_id},
            Key=new_key
        )
        print("Copy successful.")
        return key
    except Exception as e:
        print(f"Failed to copy {key}: {e}")
        return None

for page in page_iterator:
    versions = page.get('Versions', [])
    for version in versions:
        restore_tasks.append(version)

restored_count = 0
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(restore_object, v) for v in restore_tasks]
    for future in as_completed(futures):
        result = future.result()
        if result:
            restored_count += 1

print("\n Restore complete.")
print(f"Total files restored: {restored_count}")
