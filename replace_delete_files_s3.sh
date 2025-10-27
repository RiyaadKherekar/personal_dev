set -euo pipefail

SRC_BUCKET="ct-ire-edp-prd-datastaging-op"
SRC_PREFIX="compass-backup/replace-this-with-current-compass"
DST_BUCKET="ct-ire-edp-prd-datastaging-op"
DST_PREFIX="changeaudit/compass"

TABLES=(
        "compass_case_bills"
        "compass_case_seq_rules"
        "compass_organization"
        "compass_dd_rjct_hist"
)

KEEP_PARTITIONS_GLOBAL=(
        "cdc_date=2025-09-29"
)

DRY_RUN=false

EXCLUDE_ARGS=()
for p in "${KEEP_PARTITIONS_GLOBAL[@]}"; do
        EXCLUDE_ARGS+=( --exclude "${p}*" )
done

for table in "${TABLES[@]}"; do
        SRC="s3://${SRC_BUCKET}/${SRC_PREFIX}/${table}/"
        DST="s3://${DST_BUCKET}/${DST_PREFIX}/${table}/"

        echo "------------------------------------------------------"
        echo "Syncing:  $SRC  -->  $DST"
        echo "Protecting partitions: ${KEEP_PARTITIONS_GLOBAL[*]:-"<none>"}"
        echo "------------------------------------------------------"

        if [[ "${DRY_RUN}" == "true" ]]; then
                aws s3 sync "$SRC" "$DST" --delete --dryrun "${EXCLUDE_ARGS[@]}"
        else
                aws s3 sync "$SRC" "$DST" --delete "${EXCLUDE_ARGS[@]}"
        fi
done

echo "All table syncs complete."
