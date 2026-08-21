#!/usr/bin/env bash
#
# Rebuilds the ZFS dataset chain for client 'buero' into a clean, UrBackup-compliant
# linear clone chain (D1@ro -> D2@ro -> D3@ro ...) using the already repaired local
# datasets as the source.
#
# End state:
# - data/urbackup/files/buero/251121-1218 (root dataset, snapshot @ro)
# - data/urbackup/files/buero/251203-1611 (clone of 251121-1218@ro, snapshot @ro)
# - ...
# - Every dataset has exactly ONE snapshot: @ro
# - All datasets mounted cleanly at /data/urbackup/files/buero/<backup_name>
#

set -Eeuo pipefail

readonly POOL_PARENT="data/urbackup/files"
readonly CLIENT="buero"
readonly NEW_PARENT="${POOL_PARENT}/${CLIENT}.new"
readonly TARGET_PARENT="${POOL_PARENT}/${CLIENT}"
readonly MOUNT_BASE="/data/urbackup/files"

apply=0
do_swap=0

usage() {
    cat <<EOF
Usage: ${0##*/} [options]

Rebuilds the buero ZFS clone chain into a standard UrBackup linear structure.

Options:
  --apply       Perform dataset creation and rsync delta synchronization
  --swap        Destroy old tangled datasets and activate the new clean chain
  -h, --help    Show this help
EOF
}

while (($#)); do
    case "$1" in
        --apply)
            apply=1
            shift
            ;;
        --swap)
            do_swap=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
    esac
done

log() {
    printf '%(%F %T)T: %s\n' -1 "$*"
}

run() {
    if (( apply )); then
        log "RUN: $(printf '%q ' "$@")"
        "$@"
    else
        log "DRY-RUN: $(printf '%q ' "$@")"
    fi
}

readonly BACKUPS=(
    "251121-1218"
    "251203-1611"
    "260126-1015"
    "260129-1132"
    "260330-0837"
    "260413-1503"
    "260601-1029"
    "260603-1141"
    "260604-1152"
    "260605-1206"
    "260608-1132"
    "260609-1145"
    "260610-1504"
    "260612-1132"
    "260615-1132"
    "260629-0857"
    "260701-1600"
    "260703-1500"
    "260707-1504"
    "260708-1523"
    "260709-1540"
    "260714-1501"
    "260803-1131"
    "260804-1222"
)

rebuild_chain() {
    log "Starting chain rebuild for '${CLIENT}' under '${NEW_PARENT}'..."
    
    # 1. Create parent container if needed
    if ! zfs list -H -o name "${NEW_PARENT}" >/dev/null 2>&1; then
        run zfs create -o mountpoint=none "${NEW_PARENT}"
    fi

    local prev_backup=""
    local idx=0

    for curr_backup in "${BACKUPS[@]}"; do
        local curr_ds="${NEW_PARENT}/${curr_backup}"
        local curr_mount="${MOUNT_BASE}/${CLIENT}.new/${curr_backup}"
        local src_mount="${MOUNT_BASE}/${CLIENT}/${curr_backup}"

        log "=== Processing backup $((idx + 1))/${#BACKUPS[@]}: ${curr_backup} ==="

        if zfs list -H -o name "${curr_ds}@ro" >/dev/null 2>&1; then
            log "Dataset '${curr_ds}@ro' already exists, skipping."
            prev_backup="${curr_backup}"
            ((idx += 1))
            continue
        fi

        if [[ -z "${prev_backup}" ]]; then
            # First root dataset
            if ! zfs list -H -o name "${curr_ds}" >/dev/null 2>&1; then
                run zfs create -o mountpoint="${curr_mount}" "${curr_ds}"
            fi
            log "Syncing initial root dataset from ${src_mount}/ to ${curr_mount}/"
            if (( apply )); then
                rsync -aHAXS --numeric-ids --info=progress2 "${src_mount}/" "${curr_mount}/"
            else
                run rsync -aHAXS --numeric-ids "${src_mount}/" "${curr_mount}/"
            fi
        else
            # Subsequent clone
            local prev_ds="${NEW_PARENT}/${prev_backup}"
            if ! zfs list -H -o name "${curr_ds}" >/dev/null 2>&1; then
                run zfs clone -o mountpoint="${curr_mount}" "${prev_ds}@ro" "${curr_ds}"
            fi
            log "Syncing delta from ${src_mount}/ to ${curr_mount}/"
            if (( apply )); then
                rsync -aHAXS --numeric-ids --inplace --delete --info=progress2 "${src_mount}/" "${curr_mount}/"
            else
                run rsync -aHAXS --numeric-ids --inplace --delete "${src_mount}/" "${curr_mount}/"
            fi
        fi

        log "Creating readonly snapshot ${curr_ds}@ro"
        run zfs snapshot "${curr_ds}@ro"

        prev_backup="${curr_backup}"
        ((idx += 1))
    done

    # Optional empty/trailing dataset 260817-1543 if it existed
    if zfs list -H -o name "${TARGET_PARENT}/260817-1543" >/dev/null 2>&1; then
        local trailing_ds="${NEW_PARENT}/260817-1543"
        local trailing_mount="${MOUNT_BASE}/${CLIENT}.new/260817-1543"
        if ! zfs list -H -o name "${trailing_ds}" >/dev/null 2>&1; then
            log "Creating trailing un-snapshotted clone ${trailing_ds} from ${NEW_PARENT}/${prev_backup}@ro"
            run zfs clone -o mountpoint="${trailing_mount}" "${NEW_PARENT}/${prev_backup}@ro" "${trailing_ds}"
        fi
    fi

    log "Rebuild phase complete."
}

swap_chain() {
    log "Performing swap to activate clean chain..."

    if ! zfs list -H -o name "${NEW_PARENT}" >/dev/null 2>&1; then
        log "ERROR: ${NEW_PARENT} does not exist. Run rebuild first." >&2
        exit 1
    fi

    # Verify all 24 @ro snapshots exist in NEW_PARENT
    for b in "${BACKUPS[@]}"; do
        if ! zfs list -H -o name "${NEW_PARENT}/${b}@ro" >/dev/null 2>&1; then
            log "ERROR: Missing snapshot ${NEW_PARENT}/${b}@ro. Refusing to swap." >&2
            exit 1
        fi
    done

    # 1. Unmount and destroy old messy client datasets
    log "Destroying old messy dataset tree ${TARGET_PARENT}..."
    if (( apply )); then
        # Unmount old datasets if mounted
        zfs list -H -o name -r "${TARGET_PARENT}" | tac | while IFS= read -r ds; do
            zfs unmount "$ds" 2>/dev/null || true
        done
        zfs destroy -r "${TARGET_PARENT}"
    else
        log "DRY-RUN: zfs destroy -r ${TARGET_PARENT}"
    fi

    # 2. Rename new parent to target
    log "Renaming ${NEW_PARENT} to ${TARGET_PARENT}..."
    run zfs rename "${NEW_PARENT}" "${TARGET_PARENT}"

    # 3. Fix mountpoints to standard /data/urbackup/files/buero/<backup>
    log "Setting standard mountpoints and mounting..."
    for b in "${BACKUPS[@]}"; do
        local target_ds="${TARGET_PARENT}/${b}"
        local final_mount="${MOUNT_BASE}/${CLIENT}/${b}"
        run zfs set mountpoint="${final_mount}" "${target_ds}"
        if (( apply )); then
            zfs mount "${target_ds}" 2>/dev/null || true
        fi
    done

    if zfs list -H -o name "${TARGET_PARENT}/260817-1543" >/dev/null 2>&1; then
        run zfs set mountpoint="${MOUNT_BASE}/${CLIENT}/260817-1543" "${TARGET_PARENT}/260817-1543"
        if (( apply )); then
            zfs mount "${TARGET_PARENT}/260817-1543" 2>/dev/null || true
        fi
    fi

    log "Swap complete! Chain is now active and UrBackup-compliant."
}

rebuild_chain

if (( do_swap )); then
    swap_chain
else
    log "Run with --swap to activate the rebuilt chain."
fi
