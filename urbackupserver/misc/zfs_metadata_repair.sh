#!/usr/bin/env bash
#
# Rebuild ZFS-backed UrBackup file backups whose dataset trees contain entries
# that cannot be traversed after a recovery with zfs_send_corrupt_data=1.
#
# The script deliberately has no SQLite dependency. It repairs only the ZFS
# storage layer; reconcile-storage can subsequently remove database rows for
# files that could not be copied. Keeping these operations separate makes the
# storage migration reversible until the database cleanup is explicitly run.

set -Eeuo pipefail
shopt -s nullglob

readonly SCRIPT_NAME=${0##*/}
readonly BACKUP_NAME_RE='^[0-9]{6}-[0-9]{4}(_Image_.+)?$'

apply=0
scan_only=0
prefer_newer_base=0
promote_repairs=0
readonly DATASET_FILE_CONFIG=/etc/urbackup/dataset_file
dataset_parent=""
state_root=""
staging_mount_root=""
force_rescan=0
declare -a requested_clients=()

usage()
{
	cat <<EOF
Usage: $SCRIPT_NAME [options]

Scans UrBackup file-backup datasets for unreadable metadata and rebuilds each
contiguous affected range from a neighbouring clean @ro snapshot. The default
is scan-only/dry-run; pass --apply to perform the migration.

The parent dataset is read from /etc/urbackup/dataset_file, matching the
configured UrBackup file-backup dataset hierarchy, unless explicitly
overridden for a test or a separate storage tree.

Options:
  --dataset-parent <dataset>  Override the configured parent dataset
  --client <name>             Restrict processing to one client (repeatable)
  --apply                     Create replacement datasets and swap them in
  --scan-only                 Scan and write the plan without rebuilding
  --prefer-newer-base         If both neighbours are clean, rebuild backwards
  --promote-repairs           Promote each replacement after the swap. This
                              detaches its clone lineage but can transfer
                              snapshot ownership from its clean base.
  --rescan                    Ignore cached scan results in the state root
  --state-root <directory>    Store logs and plans here (default: <parent-mount>/.zfs-metadata-repair)
  --staging-mount-root <dir>  Mount replacement clones below this directory
                              (default: <state-root>/mounts)
  -h, --help                  Show this help

The script preserves visible storage links. It renames each old dataset only
after every affected backup of its client has been rebuilt, then mounts the
replacement at exactly the old mountpoint. Archived datasets are retained as
<dataset>.__pre_metadata_repair_<run-id> by default.
EOF
}

log()
{
	printf '%(%F %T)T: %s\n' -1 "$*"
}

warn()
{
	log "WARNING: $*" >&2
}

die()
{
	log "ERROR: $*" >&2
	exit 1
}

run()
{
	if (( apply )); then
		log "RUN: $(printf '%q ' "$@")"
		"$@"
	else
		log "DRY-RUN: $(printf '%q ' "$@")"
	fi
}

while (($#)); do
	case "$1" in
	--dataset-parent)
		(($# >= 2)) || die "--dataset-parent requires a value"
		dataset_parent=$2
		shift 2
		;;
	--client)
		(($# >= 2)) || die "--client requires a value"
		requested_clients+=("$2")
		shift 2
		;;
	--apply)
		apply=1
		shift
		;;
	--scan-only)
		scan_only=1
		shift
		;;
	--prefer-newer-base)
		prefer_newer_base=1
		shift
		;;
	--promote-repairs)
		promote_repairs=1
		shift
		;;
	--rescan)
		force_rescan=1
		shift
		;;
	--state-root)
		(($# >= 2)) || die "--state-root requires a value"
		state_root=$2
		shift 2
		;;
	--staging-mount-root)
		(($# >= 2)) || die "--staging-mount-root requires a value"
		staging_mount_root=$2
		shift 2
		;;
	-h|--help)
		usage
		exit 0
		;;
	*)
		die "Unknown option: $1"
		;;
	esac
done

# The normal path derives the parent from UrBackup's own configuration. An
# explicit option remains useful for isolated test pools and takes precedence
# without changing the configuration used by the running server.
if [[ -z $dataset_parent ]]; then
	[[ -r $DATASET_FILE_CONFIG ]] || die "Cannot read configured file-backup dataset parent: $DATASET_FILE_CONFIG"
	dataset_parent=$(awk 'NF && $1 !~ /^#/ { print $1; exit }' "$DATASET_FILE_CONFIG")
	[[ -n $dataset_parent ]] || die "No file-backup dataset parent configured in $DATASET_FILE_CONFIG"
fi

for required_command in zfs du rsync awk sort; do
	command -v "$required_command" >/dev/null || die "Required command not found: $required_command"
done

zfs list -H -o name "$dataset_parent" >/dev/null 2>&1 || die "ZFS dataset does not exist or is inaccessible: $dataset_parent"

run_id=$(date +%Y%m%d-%H%M%S)
if [[ -z $state_root ]]; then
	parent_mountpoint=$(zfs get -H -o value mountpoint "$dataset_parent")
	[[ $parent_mountpoint != '-' && $parent_mountpoint != none && -n $parent_mountpoint ]] || die "Parent dataset has no usable mountpoint: $dataset_parent"
	# Persist scan plans beneath the configured parent dataset so consecutive
	# dry-runs and apply runs share one review location without timestamped paths.
	state_root="$parent_mountpoint/.zfs-metadata-repair"
fi
if [[ -z $staging_mount_root ]]; then
	staging_mount_root="$state_root/mounts"
fi

# State files make the scan result auditable and allow the exact set of failed
# paths to be reviewed before an apply run. They are never read as shell code.
mkdir -p "$state_root" "$staging_mount_root"
readonly all_datasets_file="$state_root/all-datasets.tsv"
readonly scan_errors_file="$state_root/scan-errors.log"
readonly unreadable_paths_file="$state_root/unreadable-paths.tsv"
readonly bad_datasets_file="$state_root/bad-datasets.tsv"
readonly repair_plan_file="$state_root/repair-plan.tsv"
readonly archived_properties_file="$state_root/archived-dataset-properties.tsv"
readonly scan_context_file="$state_root/scan-context"
readonly active_run_id_file="$state_root/active-run-id"
interrupted=0

initialize_apply_run()
{
	local persisted_run_id

	(( apply )) || return 0
	if [[ -e $active_run_id_file ]]; then
		IFS= read -r persisted_run_id < "$active_run_id_file" || die "Cannot read interrupted run identifier: $active_run_id_file"
		[[ $persisted_run_id =~ ^[0-9]{8}-[0-9]{6}$ ]] || die "Invalid interrupted run identifier in $active_run_id_file"
		# Keep replacement dataset names stable after an interruption. This makes
		# rsync continue into the same clone instead of creating another copy.
		run_id=$persisted_run_id
		log "Resuming interrupted repair run: $run_id"
	else
		printf '%s\n' "$run_id" > "$active_run_id_file"
		log "Starting repair run: $run_id"
	fi
}

handle_interrupt()
{
	# The signal handler must not remove state: the active run ID and incomplete
	# clone are the durable resume point once the interrupted child exits.
	interrupted=1
}

abort_if_interrupted()
{
	if (( interrupted )); then
		warn "Interrupted. No further datasets will be processed; rerun with the same arguments to resume incomplete replacements."
		exit 130
	fi
}

reset_scan_state()
{
	: > "$all_datasets_file"
	: > "$scan_errors_file"
	: > "$unreadable_paths_file"
	: > "$bad_datasets_file"
}

reset_plan_state()
{
	: > "$repair_plan_file"
	: > "$archived_properties_file"
}

write_scan_context()
{
	local requested_client

	# The scan cache is only safe to reuse when the dataset root and client
	# filter match. The context file records exactly that and is treated as
	# plain data, never as shell code.
	{
		printf 'scanned_at=%(%F %T)T\n' -1
		printf 'dataset_parent=%s\n' "$dataset_parent"
		for requested_client in "${requested_clients[@]}"; do
			printf 'client=%s\n' "$requested_client"
		done
	} > "$scan_context_file"
}

read_scan_timestamp()
{
	local line

	[[ -s $scan_context_file ]] || return 1
	while IFS= read -r line; do
		case "$line" in
		scanned_at=*)
			printf '%s\n' "${line#scanned_at=}"
			return 0
			;;
		esac
	done < "$scan_context_file"

	return 1
}

scan_context_matches()
{
	local line expected_clients serialized_expected serialized_existing
	local -a existing_clients=()

	[[ -s $scan_context_file ]] || return 1
	while IFS= read -r line; do
		case "$line" in
		scanned_at=*)
			;;
		dataset_parent=*)
			[[ ${line#dataset_parent=} == "$dataset_parent" ]] || return 1
			;;
		client=*)
			existing_clients+=("${line#client=}")
			;;
		*)
			return 1
			;;
		esac
	done < "$scan_context_file"

	serialized_expected=$(printf '%s\n' "${requested_clients[@]}" | LC_ALL=C sort)
	serialized_existing=$(printf '%s\n' "${existing_clients[@]}" | LC_ALL=C sort)
	[[ $serialized_expected == "$serialized_existing" ]]
}

should_reuse_scan_state()
{
	(( force_rescan == 0 )) || return 1
	(( apply == 1 )) || return 1
	(( scan_only == 0 )) || return 1
	scan_context_matches || return 1
	[[ -f $all_datasets_file && -f $bad_datasets_file && -f $scan_errors_file && -f $unreadable_paths_file ]] || return 1
	return 0
}

initialize_apply_run
trap handle_interrupt INT TERM
reset_plan_state

client_is_requested()
{
	local client=$1 requested_client
	((${#requested_clients[@]} == 0)) && return 0
	for requested_client in "${requested_clients[@]}"; do
		[[ $client == "$requested_client" ]] && return 0
	done
	return 1
}

dataset_snapshot_exists()
{
	zfs list -H -t snapshot -o name "$1@ro" >/dev/null 2>&1
}

archive_old_dataset()
{
	local source_dataset=$1 archived_dataset=$2 source_mountpoint=$3
	local old_mountpoint old_canmount

	if (( apply )); then
		old_mountpoint=$(zfs get -H -o value mountpoint "$source_dataset")
		old_canmount=$(zfs get -H -o value canmount "$source_dataset")
		printf '%s\t%s\t%s\t%s\n' "$source_dataset" "$archived_dataset" "$old_mountpoint" "$old_canmount" >> "$archived_properties_file"
	fi

	# An explicitly configured mountpoint survives zfs rename. Disabling mounts
	# on the archived rollback copy guarantees that the unchanged storage link
	# can resolve to the replacement at its original target path.
	run zfs rename "$source_dataset" "$archived_dataset"
	if (( apply == 0 )) || [[ $(zfs get -H -o value mounted "$archived_dataset") == yes ]]; then
		# zfs unmount reports an error for an already unmounted archive, which is
		# harmless but must not abort an otherwise valid replacement swap.
		run zfs unmount "$archived_dataset"
	fi
	run zfs set canmount=off "$archived_dataset"
	run zfs set mountpoint=none "$archived_dataset"
}

scan_dataset()
{
	local dataset=$1 mountpoint=$2 status_file="$state_root/scan-status.$(printf '%s' "$dataset" | tr '/ ' '__').log"
	local scan_rc=0

	log "Scanning metadata tree of dataset \"$dataset\" at \"$mountpoint\""
	# GNU du performs a metadata walk without following symlinks. This catches
	# ZFS EIO errors in directories and inode metadata while avoiding Windows
	# compatibility links such as 'Dokumente und Einstellungen'.
	LC_ALL=C du -a -x --apparent-size -- "$mountpoint" >/dev/null 2>"$status_file" || scan_rc=$?
	if (( scan_rc != 0 )); then
		warn "Metadata scan found unreadable entries in dataset \"$dataset\"; details: $status_file"
		printf '%s\t%s\n' "$dataset" "$mountpoint" >> "$bad_datasets_file"
		{
			printf '### dataset=%s mountpoint=%s\n' "$dataset" "$mountpoint"
			cat "$status_file"
		} >> "$scan_errors_file"
		# Preserve the original diagnostic instead of parsing locale- and filename-
		# dependent du messages. Each row is a reviewable affected path indication
		# tied to its dataset, including errors for unreadable directories.
		while IFS= read -r scan_message; do
			printf '%s\t%s\t%s\n' "$dataset" "$mountpoint" "$scan_message" >> "$unreadable_paths_file"
		done < "$status_file"
	else
		rm -f "$status_file"
	fi
}

collect_datasets()
{
	local dataset mountpoint relative client backup
	while IFS=$'\t' read -r dataset mountpoint; do
		# Only mounted descendant filesystems can represent a visible backup.
		[[ $dataset != "$dataset_parent" && $mountpoint != '-' && -d $mountpoint ]] || continue
		relative=${dataset#"$dataset_parent"/}
		[[ $relative != "$dataset" ]] || continue
		client=${relative%%/*}
		backup=${relative##*/}
		client_is_requested "$client" || continue
		[[ $backup =~ $BACKUP_NAME_RE ]] || continue
		printf '%s\t%s\t%s\t%s\n' "$client" "$backup" "$dataset" "$mountpoint" >> "$all_datasets_file"
	done < <(zfs list -H -p -t filesystem -o name,mountpoint -r "$dataset_parent")

	sort -t $'\t' -k1,1 -k2,2 "$all_datasets_file" -o "$all_datasets_file"
}

is_bad_dataset()
{
	local dataset=$1
	awk -F '\t' -v dataset="$dataset" '$1 == dataset { found=1 } END { exit !found }' "$bad_datasets_file"
}

append_plan()
{
	local client=$1 backup=$2 source_dataset=$3 source_mountpoint=$4 base_dataset=$5 direction=$6

	# Fields: client, backup, source dataset, source mountpoint, base dataset,
	# direction. A literal '-' represents an absent base: Bash read treats tabs as
	# whitespace and would otherwise collapse the empty TSV field, shifting the
	# direction "empty" into base_dataset during the later apply run.
	[[ -n $base_dataset ]] || base_dataset=-
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$client" "$backup" "$source_dataset" "$source_mountpoint" "$base_dataset" "$direction" >> "$repair_plan_file"
}

build_client_plan()
{
	local client=$1
	local -a backups=() datasets=() mountpoints=() bad=()
	local line backup dataset mountpoint i start end base_index direction

	while IFS=$'\t' read -r _client backup dataset mountpoint; do
		[[ $_client == "$client" ]] || continue
		backups+=("$backup")
		datasets+=("$dataset")
		mountpoints+=("$mountpoint")
		if is_bad_dataset "$dataset"; then
			bad+=(1)
		else
			bad+=(0)
		fi
	done < "$all_datasets_file"

	for ((i = 0; i < ${#backups[@]};)); do
		if ((bad[i] == 0)); then
			((i += 1))
			continue
		fi
		start=$i
		while ((i + 1 < ${#backups[@]} && bad[i + 1] == 1)); do
			((i += 1))
		done
		end=$i

		# Prefer a clean predecessor because UrBackup's normal dataset history is
		# chronological. A clean successor is used when it is the only available
		# base, or explicitly preferred; this reconstructs the range backwards.
		if ((start > 0)) && dataset_snapshot_exists "${datasets[start - 1]}" && \
			{ ((prefer_newer_base == 0)) || ((end + 1 >= ${#backups[@]})) || ! dataset_snapshot_exists "${datasets[end + 1]}"; }; then
			base_index=$((start - 1))
			direction=forward
		elif ((end + 1 < ${#backups[@]})) && dataset_snapshot_exists "${datasets[end + 1]}"; then
			base_index=$((end + 1))
			direction=backward
		else
			base_index=-1
			direction=empty
		fi

		if [[ $direction == forward ]]; then
			for ((line = start; line <= end; line += 1)); do
				if ((line == start)); then
					append_plan "$client" "${backups[line]}" "${datasets[line]}" "${mountpoints[line]}" "${datasets[base_index]}" "$direction"
				else
					append_plan "$client" "${backups[line]}" "${datasets[line]}" "${mountpoints[line]}" "${datasets[line - 1]}.repair.$run_id" "$direction"
				fi
			done
		elif [[ $direction == backward ]]; then
			for ((line = end; line >= start; line -= 1)); do
				if ((line == end)); then
					append_plan "$client" "${backups[line]}" "${datasets[line]}" "${mountpoints[line]}" "${datasets[base_index]}" "$direction"
				else
					append_plan "$client" "${backups[line]}" "${datasets[line]}" "${mountpoints[line]}" "${datasets[line + 1]}.repair.$run_id" "$direction"
				fi
			done
		else
			for ((line = start; line <= end; line += 1)); do
				if ((line == start)); then
					append_plan "$client" "${backups[line]}" "${datasets[line]}" "${mountpoints[line]}" "" "$direction"
				else
					append_plan "$client" "${backups[line]}" "${datasets[line]}" "${mountpoints[line]}" "${datasets[line - 1]}.repair.$run_id" "$direction"
				fi
			done
		fi
		((i += 1))
	done
}

create_empty_repair_dataset()
{
	local replacement_dataset=$1 replacement_mountpoint=$2
	# A clean base does not exist for this range, so an empty filesystem is the
	# only safe starting point. ZFS deduplication can still share matching blocks
	# once rsync populates it, but no potentially unreadable metadata is inherited.
	run zfs create -o mountpoint="$replacement_mountpoint" "$replacement_dataset"
}

create_repair_dataset()
{
	local base_dataset=$1 replacement_dataset=$2 replacement_mountpoint=$3
	if [[ -n $base_dataset ]]; then
		dataset_snapshot_exists "$base_dataset" || die "Required clean base snapshot is missing: $base_dataset@ro"
		# The replacement starts as a clone of a verified neighbour. rsync without
		# --inplace then leaves inherited target data intact if a source read fails.
		run zfs clone -o mountpoint="$replacement_mountpoint" "$base_dataset@ro" "$replacement_dataset"
	else
		create_empty_repair_dataset "$replacement_dataset" "$replacement_mountpoint"
	fi
}

copy_dataset_tree()
{
	local source_mountpoint=$1 replacement_mountpoint=$2 copy_log=$3
	local rsync_rc=0
	local -a copy_pipeline_status=()

	log "Copying \"$source_mountpoint\" to \"$replacement_mountpoint\""
	if (( apply )); then
		# Do not use --inplace or --delete: source EIO must neither partially
		# overwrite an inherited good file nor make an unreadable source directory
		# look empty. Successfully read objects are updated; uncertain old objects
		# remain until a later verification can decide whether they were deleted.
		# progress2 reports the complete rsync transfer, while tee retains progress
		# and read errors in the per-backup log for later review.
		set +e
		LC_ALL=C rsync -aHAXS --numeric-ids --ignore-errors --info=progress2 -- "$source_mountpoint/" "$replacement_mountpoint/" 2>&1 | tee "$copy_log"
		copy_pipeline_status=("${PIPESTATUS[@]}")
		set -e
		rsync_rc=${copy_pipeline_status[0]}
		if (( copy_pipeline_status[1] != 0 )); then
			die "Could not write rsync output log: $copy_log"
		fi
		if (( rsync_rc == 20 || rsync_rc == 130 )); then
			# rsync uses 20 for a received SIGINT. Treating it like a tolerated read
			# error would snapshot a partial clone and incorrectly start the next one.
			interrupted=1
		fi
		abort_if_interrupted
		if (( rsync_rc != 0 )); then
			warn "rsync returned $rsync_rc for \"$source_mountpoint\"; unreadable entries were not copied. Details: $copy_log"
		fi
	else
		log "DRY-RUN: rsync -aHAXS --numeric-ids --ignore-errors --info=progress2 -- $(printf '%q' "$source_mountpoint/") $(printf '%q' "$replacement_mountpoint/")"
	fi
}

snapshot_replacement()
{
	local replacement_dataset=$1
	# Each rebuilt dataset receives the same @ro checkpoint expected by UrBackup
	# and becomes the clone base for the next affected backup in this range.
	run zfs snapshot "$replacement_dataset@ro"
}

prepare_staging_mountpoint()
{
	local replacement_mountpoint=$1
	local -a staging_entries=()

	if [[ -e $replacement_mountpoint || -L $replacement_mountpoint ]]; then
		[[ -d $replacement_mountpoint && ! -L $replacement_mountpoint ]] || die "Staging mountpoint is not an ordinary directory: $replacement_mountpoint"
		staging_entries=("$replacement_mountpoint"/* "$replacement_mountpoint"/.[!.]* "$replacement_mountpoint"/..?*)
		# A failed run can leave mkdir -p's empty mountpoint behind before ZFS
		# creates a replacement. Reusing only an empty plain directory is safe;
		# content here could instead be a partially mounted or copied dataset.
		((${#staging_entries[@]} == 0)) || die "Staging mountpoint is not empty and will not be reused: $replacement_mountpoint"
		log "Reusing empty staging mountpoint left by an earlier interrupted run: $replacement_mountpoint"
	else
		mkdir -p "$replacement_mountpoint"
	fi
}

resume_replacement_dataset()
{
	local replacement_dataset=$1 replacement_mountpoint=$2
	local replacement_mountpoint_property replacement_mounted

	replacement_mountpoint_property=$(zfs get -H -o value mountpoint "$replacement_dataset")
	[[ $replacement_mountpoint_property == "$replacement_mountpoint" ]] || die "Existing replacement dataset has an unexpected mountpoint: $replacement_dataset ($replacement_mountpoint_property)"
	replacement_mounted=$(zfs get -H -o value mounted "$replacement_dataset")
	if [[ $replacement_mounted != yes ]]; then
		# Recreate only the ordinary mountpoint directory before mounting the known
		# replacement. A mounted dataset is never inspected as an empty directory.
		mkdir -p "$replacement_mountpoint"
		run zfs mount "$replacement_dataset"
	fi
	log "Resuming incomplete replacement dataset: $replacement_dataset"
}

rebuild_client()
{
	local client=$1 client_plan="$state_root/repair-plan.$client.tsv"
	local plan_client backup source_dataset source_mountpoint base_dataset direction
	local replacement_dataset replacement_mountpoint copy_log

	awk -F '\t' -v client="$client" '$1 == client { print }' "$repair_plan_file" > "$client_plan"
	[[ -s $client_plan ]] || return 0

	log "Rebuilding affected backup datasets for client \"$client\""
	while IFS=$'\t' read -r plan_client backup source_dataset source_mountpoint base_dataset direction; do
		abort_if_interrupted
		# See append_plan: restore the explicit empty-base marker before choosing
		# between a ZFS clone and a freshly created replacement filesystem.
		[[ $base_dataset != - ]] || base_dataset=""
		replacement_dataset="$source_dataset.repair.$run_id"
		replacement_mountpoint="$staging_mount_root/$client/$backup"
		copy_log="$state_root/rsync.$client.$backup.log"
		if (( apply )); then
			if zfs list -H -o name "$replacement_dataset" >/dev/null 2>&1; then
				if dataset_snapshot_exists "$replacement_dataset"; then
					# A completed replacement already has its immutable checkpoint and can
					# supply the next clone in the chain without copying it again.
					log "Replacement dataset is already complete; skipping copy: $replacement_dataset"
					continue
				fi
				resume_replacement_dataset "$replacement_dataset" "$replacement_mountpoint"
			else
				prepare_staging_mountpoint "$replacement_mountpoint"
				create_repair_dataset "$base_dataset" "$replacement_dataset" "$replacement_mountpoint"
			fi
		else
			create_repair_dataset "$base_dataset" "$replacement_dataset" "$replacement_mountpoint"
		fi
		copy_dataset_tree "$source_mountpoint" "$replacement_mountpoint" "$copy_log"
		abort_if_interrupted
		snapshot_replacement "$replacement_dataset"
	done < "$client_plan"
}

swap_client_datasets()
{
	local client=$1 client_plan="$state_root/repair-plan.$client.tsv"
	local plan_client backup source_dataset source_mountpoint base_dataset direction
	local replacement_dataset archived_dataset

	[[ -s $client_plan ]] || return 0
	log "Swapping rebuilt datasets for client \"$client\" while preserving existing storage links"
	while IFS=$'\t' read -r plan_client backup source_dataset source_mountpoint base_dataset direction; do
		abort_if_interrupted
		replacement_dataset="$source_dataset.repair.$run_id"
		archived_dataset="$source_dataset.__pre_metadata_repair_$run_id"
		zfs list -H -o name "$replacement_dataset" >/dev/null 2>&1 || die "Replacement dataset is missing: $replacement_dataset"

		# The archive is made non-mountable before assigning the old target to the
		# replacement. Existing storage links therefore keep their exact target.
		archive_old_dataset "$source_dataset" "$archived_dataset" "$source_mountpoint"
		run zfs set mountpoint="$source_mountpoint" "$replacement_dataset"
		run zfs mount "$replacement_dataset"

		if (( promote_repairs )); then
			# Promotion detaches the replacement from the archived lineage. ZFS may
			# transfer ownership of the origin snapshot from a clean base, so it is
			# opt-in and must be reviewed with the generated repair plan beforehand.
			run zfs promote "$replacement_dataset"
		fi
	done < "$client_plan"
}

if should_reuse_scan_state; then
	# Keep cache reuse independent of an optional timestamp. Older cache files
	# from before the timestamp field remain valid and are reported as unknown.
	if ! scan_timestamp=$(read_scan_timestamp 2>/dev/null); then
		scan_timestamp=unknown
	fi
	# A prior dry-run already captured the affected datasets for the same scope.
	# Reusing it keeps the later apply run aligned with the reviewed plan instead
	# of traversing the tree again and potentially taking hours a second time.
	log "Reusing existing scan result from state directory: $state_root (created at $scan_timestamp)"
else
	reset_scan_state
	collect_datasets
	if [[ ! -s $all_datasets_file ]]; then
		warn "No matching backup datasets found below $dataset_parent"
		exit 0
	fi

	log "Running fresh metadata scan. Use --rescan later to explicitly refresh this state."
	while IFS=$'\t' read -r client backup dataset mountpoint; do
		scan_dataset "$dataset" "$mountpoint"
		abort_if_interrupted
	done < "$all_datasets_file"
	write_scan_context
fi

if [[ ! -s $bad_datasets_file ]]; then
	log "No metadata traversal errors found. State directory: $state_root"
	exit 0
fi

while IFS=$'\t' read -r client; do
	build_client_plan "$client"
done < <(cut -f1 "$all_datasets_file" | sort -u)

log "Affected datasets: $(wc -l < "$bad_datasets_file")"
log "Repair plan: $repair_plan_file"
log "Scan errors: $scan_errors_file"
log "Unreadable path indications: $unreadable_paths_file"
if (( apply == 0 || scan_only )); then
	log "Dry-run complete. Review the plan and rerun with --apply to create replacement datasets."
	exit 0
fi

while IFS=$'\t' read -r client; do
	abort_if_interrupted
	rebuild_client "$client"
done < <(cut -f1 "$repair_plan_file" | sort -u)

while IFS=$'\t' read -r client; do
	abort_if_interrupted
	swap_client_datasets "$client"
done < <(cut -f1 "$repair_plan_file" | sort -u)

log "Migration complete. Old datasets remain archived as a rollback path."
if (( apply )); then
	# Remove the resume marker only after every replacement was swapped. Until
	# then, an interruption must preserve the same replacement dataset names.
	rm -f "$active_run_id_file"
fi
log "Archived dataset mountpoint properties: $archived_properties_file"
if (( promote_repairs == 0 )); then
	warn "Replacement datasets remain ZFS clones. Do not destroy archived datasets before explicitly reviewing and promoting the repaired clone chain."
fi
log "Next step after verifying mounts and data: urbackupsrv reconcile-storage --remove-missing-files --drop-empty-backups --apply"
