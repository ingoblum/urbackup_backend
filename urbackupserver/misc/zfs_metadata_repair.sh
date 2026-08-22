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
rsync_inplace=0
delete_archived_repairs=0
list_clients_mode=0
readonly DATASET_FILE_CONFIG=/etc/urbackup/dataset_file
dataset_parent=""
state_root=""
staging_mount_root=""
force_rescan=0
declare -a requested_clients=()
declare -a requested_client_regexes=()

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
  --client-regex <regex>      Restrict processing by regex pattern (repeatable)
  --list-clients              List matched clients for filters and exit
  --apply                     Create replacement datasets and swap them in
  --scan-only                 Scan and write the plan without rebuilding
  --inplace                   Pass --inplace to rsync while rebuilding
                              replacement datasets
  --delete                    After a successful swap, destroy the matching
                              archived old dataset
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
	--client-regex)
		(($# >= 2)) || die "--client-regex requires a value"
		requested_client_regexes+=("$2")
		shift 2
		;;
	--list-clients)
		list_clients_mode=1
		shift
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
		# Kept for backward compatibility
		shift
		;;
	--inplace)
		rsync_inplace=1
		shift
		;;
	--delete)
		delete_archived_repairs=1
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

for required_command in zfs du rsync awk sort findmnt; do
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
resuming_apply_run=0

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
		resuming_apply_run=1
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
	# If we are resuming an active apply run and an existing repair plan exists,
	# keep it so an interrupted run continues executing the exact same plan.
	if (( resuming_apply_run == 0 )) || [[ ! -s $repair_plan_file ]]; then
		: > "$repair_plan_file"
		: > "$archived_properties_file"
	fi
}

write_scan_context()
{
	local requested_client requested_regex

	# The scan cache is only safe to reuse when the dataset root and client
	# filter match. The context file records exactly that and is treated as
	# plain data, never as shell code.
	{
		printf 'scanned_at=%(%F %T)T\n' -1
		printf 'dataset_parent=%s\n' "$dataset_parent"
		for requested_client in "${requested_clients[@]}"; do
			printf 'client=%s\n' "$requested_client"
		done
		for requested_regex in "${requested_client_regexes[@]}"; do
			printf 'regex=%s\n' "$requested_regex"
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
	local line serialized_expected serialized_existing
	local -a existing_clients=() existing_regexes=()

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
		regex=*)
			existing_regexes+=("${line#regex=}")
			;;
		*)
			return 1
			;;
		esac
	done < "$scan_context_file"

	serialized_expected=$(printf '%s\n' "${requested_clients[@]}" "${requested_client_regexes[@]}" | LC_ALL=C sort)
	serialized_existing=$(printf '%s\n' "${existing_clients[@]}" "${existing_regexes[@]}" | LC_ALL=C sort)
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

client_is_requested()
{
	local client=$1 requested_client regex
	((${#requested_clients[@]} == 0)) && ((${#requested_client_regexes[@]} == 0)) && return 0
	for requested_client in "${requested_clients[@]}"; do
		[[ $client == "$requested_client" ]] && return 0
	done
	for regex in "${requested_client_regexes[@]}"; do
		if [[ $client =~ $regex ]]; then
			return 0
		fi
	done
	return 1
}

collect_datasets()
{
	local dataset mountpoint relative client backup
	: > "$all_datasets_file"
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

if (( list_clients_mode )); then
	collect_datasets
	if [[ ! -s $all_datasets_file ]]; then
		warn "No matching clients found below $dataset_parent"
		exit 0
	fi
	cut -f1 "$all_datasets_file" | sort -u
	exit 0
fi

initialize_apply_run
trap handle_interrupt INT TERM
reset_plan_state

dataset_snapshot_exists()
{
	zfs list -H -t snapshot -o name "$1@ro" >/dev/null 2>&1
}

snapshot_exists()
{
	zfs list -H -t snapshot -o name "$1" >/dev/null 2>&1
}

next_available_snapshot_name()
{
	local dataset=$1 snapshot_prefix=$2 candidate suffix=1

	candidate="$dataset@$snapshot_prefix"
	while snapshot_exists "$candidate"; do
		candidate="$dataset@${snapshot_prefix}_$suffix"
		((suffix += 1))
	done
	printf '%s\n' "$candidate"
}

dataset_exists()
{
	zfs list -H -o name "$1" >/dev/null 2>&1
}

record_archive_properties()
{
	local source_dataset=$1 archived_dataset=$2 old_mountpoint old_canmount

	# Do not duplicate the record when a previous attempt renamed the dataset
	# before stopping. The original properties are needed for manual rollback.
	awk -F '\t' -v archived_dataset="$archived_dataset" '$2 == archived_dataset { found=1 } END { exit !found }' "$archived_properties_file" && return 0
	old_mountpoint=$(zfs get -H -o value mountpoint "$source_dataset")
	old_canmount=$(zfs get -H -o value canmount "$source_dataset")
	printf '%s\t%s\t%s\t%s\n' "$source_dataset" "$archived_dataset" "$old_mountpoint" "$old_canmount" >> "$archived_properties_file"
}

archive_old_dataset()
{
	local source_dataset=$1 archived_dataset=$2
	local archived_mounted archived_canmount archived_mountpoint

	if dataset_exists "$source_dataset"; then
		dataset_exists "$archived_dataset" && die "Both original and archived datasets exist; refusing ambiguous swap state: $source_dataset"
		record_archive_properties "$source_dataset" "$archived_dataset"
		# An explicitly configured mountpoint survives zfs rename. The following
		# state reconciliation is deliberately separate so a busy unmount can be
		# retried without attempting the already completed rename a second time.
		run zfs rename "$source_dataset" "$archived_dataset"
	else
		dataset_exists "$archived_dataset" || die "Neither original nor archived dataset exists: $source_dataset"
		log "Original dataset is already archived; continuing swap: $archived_dataset"
	fi

	archived_mounted=$(zfs get -H -o value mounted "$archived_dataset")
	if [[ $archived_mounted == yes ]]; then
		# A busy archive remains untouched and causes a retryable failure. Its old
		# mount must not coexist with the replacement at the visible backup path.
		run zfs unmount "$archived_dataset"
	fi
	archived_canmount=$(zfs get -H -o value canmount "$archived_dataset")
	[[ $archived_canmount == off ]] || run zfs set canmount=off "$archived_dataset"
	archived_mountpoint=$(zfs get -H -o value mountpoint "$archived_dataset")
	[[ $archived_mountpoint == none ]] || run zfs set mountpoint=none "$archived_dataset"
}

activate_replacement_dataset()
{
	local replacement_dataset=$1 source_mountpoint=$2
	local replacement_mounted replacement_mountpoint actual_mountpoint

	dataset_exists "$replacement_dataset" || die "Replacement dataset is missing: $replacement_dataset"
	replacement_mounted=$(zfs get -H -o value mounted "$replacement_dataset")
	replacement_mountpoint=$(zfs get -H -o value mountpoint "$replacement_dataset")
	if [[ $replacement_mounted == yes ]]; then
		# zfs get mountpoint reports the configured property, not necessarily the
		# current kernel mount location after that property changed while mounted.
		actual_mountpoint=$(findmnt -rn -S "$replacement_dataset" -o TARGET 2>/dev/null || true)
		[[ -n $actual_mountpoint ]] || die "ZFS reports replacement as mounted but its mount target cannot be determined: $replacement_dataset"
	fi
	if [[ $replacement_mounted == yes && $actual_mountpoint == "$source_mountpoint" ]]; then
		log "Replacement dataset is already mounted at the visible backup path: $replacement_dataset"
		return 0
	fi
	if [[ $replacement_mounted == yes ]]; then
		# A previous attempt can have changed mountpoint while the clone was still
		# mounted at staging. Unmount before changing the property so ZFS mounts it
		# at the visible path on this retry instead of reporting "already mounted".
		log "Unmounting replacement from \"$actual_mountpoint\" before activation: $replacement_dataset"
		run zfs unmount "$replacement_dataset"
	fi
	[[ $replacement_mountpoint == "$source_mountpoint" ]] || run zfs set mountpoint="$source_mountpoint" "$replacement_dataset"

	# The dataset can be mounted again while its property is being updated, for
	# example by another ZFS-aware process. Recheck immediately before mounting
	# so a stale staging mount does not make the retry fail with "already mounted".
	replacement_mounted=$(zfs get -H -o value mounted "$replacement_dataset")
	if [[ $replacement_mounted == yes ]]; then
		actual_mountpoint=$(findmnt -rn -S "$replacement_dataset" -o TARGET 2>/dev/null || true)
		[[ -n $actual_mountpoint ]] || die "ZFS reports replacement as mounted but its mount target cannot be determined: $replacement_dataset"
		if [[ $actual_mountpoint == "$source_mountpoint" ]]; then
			log "Replacement dataset became mounted at the visible backup path: $replacement_dataset"
			return 0
		fi
		log "Unmounting replacement remounted at \"$actual_mountpoint\" before activation: $replacement_dataset"
		run zfs unmount "$replacement_dataset"
	fi

	log "RUN: zfs mount $replacement_dataset"
	if zfs mount "$replacement_dataset"; then
		return 0
	fi

	# If zfs mount still reports an existing mount, inspect it once more and
	# retry a normal unmount. Do not use zfs unmount -f: a busy external user
	# must remain a visible, retryable failure instead of being disrupted.
	replacement_mounted=$(zfs get -H -o value mounted "$replacement_dataset")
	if [[ $replacement_mounted == yes ]]; then
		actual_mountpoint=$(findmnt -rn -S "$replacement_dataset" -o TARGET 2>/dev/null || true)
		[[ -n $actual_mountpoint ]] || die "Replacement mount failed and its active target cannot be determined: $replacement_dataset"
		if [[ $actual_mountpoint == "$source_mountpoint" ]]; then
			log "Replacement dataset is now mounted at the visible backup path: $replacement_dataset"
			return 0
		fi
		log "Retrying replacement activation after unmounting \"$actual_mountpoint\": $replacement_dataset"
		run zfs unmount "$replacement_dataset"
		run zfs mount "$replacement_dataset"
		return 0
	fi
	die "Could not mount replacement dataset: $replacement_dataset"
}

delete_archived_repair_datasets()
{
	local candidate
	local -a pending_archives=("$@")

	# Destroy archived datasets in reverse order (leaf-to-root)
	for ((i = ${#pending_archives[@]} - 1; i >= 0; i--)); do
		candidate="${pending_archives[i]}"
		if dataset_exists "$candidate"; then
			log "Deleting archived repair dataset: $candidate"
			run zfs destroy -r "$candidate"
		fi
	done
}

scan_dataset()
{
	local dataset=$1 mountpoint=$2
	local out_bad=${3:-$bad_datasets_file}
	local out_errors=${4:-$scan_errors_file}
	local out_unreadable=${5:-$unreadable_paths_file}
	local dataset_key
	dataset_key=$(printf '%s' "$dataset" | tr '/ ' '__')
	local status_file="$state_root/scan-status.$dataset_key.log"
	local dataset_done_file="$state_root/scan-dataset.$dataset_key.done"
	local scan_rc=0 scan_message

	if (( force_rescan == 0 )) && [[ -f $dataset_done_file ]]; then
		if [[ -s $status_file ]]; then
			log "Dataset \"$dataset\": reusing cached scan result (unreadable entries found)"
			printf '%s\t%s\n' "$dataset" "$mountpoint" >> "$out_bad"
			{
				printf '### dataset=%s mountpoint=%s\n' "$dataset" "$mountpoint"
				cat "$status_file"
			} >> "$out_errors"
			while IFS= read -r scan_message; do
				printf '%s\t%s\t%s\n' "$dataset" "$mountpoint" "$scan_message" >> "$out_unreadable"
			done < "$status_file"
		else
			log "Dataset \"$dataset\": reusing cached scan result (clean)"
		fi
		return 0
	fi

	log "Scanning metadata tree of dataset \"$dataset\" at \"$mountpoint\""
	# GNU du performs a metadata walk without following symlinks. This catches
	# ZFS EIO errors in directories and inode metadata while avoiding Windows
	# compatibility links such as 'Dokumente und Einstellungen'.
	LC_ALL=C du -a -x --apparent-size -- "$mountpoint" >/dev/null 2>"$status_file" || scan_rc=$?
	if (( scan_rc != 0 )); then
		warn "Metadata scan found unreadable entries in dataset \"$dataset\"; details: $status_file"
		printf '%s\t%s\n' "$dataset" "$mountpoint" >> "$out_bad"
		{
			printf '### dataset=%s mountpoint=%s\n' "$dataset" "$mountpoint"
			cat "$status_file"
		} >> "$out_errors"
		# Preserve the original diagnostic instead of parsing locale- and filename-
		# dependent du messages. Each row is a reviewable affected path indication
		# tied to its dataset, including errors for unreadable directories.
		while IFS= read -r scan_message; do
			printf '%s\t%s\t%s\n' "$dataset" "$mountpoint" "$scan_message" >> "$out_unreadable"
		done < "$status_file"
	else
		: > "$status_file"
	fi
	touch "$dataset_done_file"
}

is_bad_dataset()
{
	local dataset=$1 client=${2:-}
	local check_file="$bad_datasets_file"
	if [[ -n $client && -f "$state_root/bad-datasets.$client.tsv" ]]; then
		check_file="$state_root/bad-datasets.$client.tsv"
	fi
	awk -F '\t' -v dataset="$dataset" '$1 == dataset { found=1 } END { exit !found }' "$check_file"
}

append_plan()
{
	local client=$1 backup=$2 source_dataset=$3 source_mountpoint=$4 base_dataset=$5 direction=$6
	local client_plan="$state_root/repair-plan.$client.tsv"

	# Fields: client, backup, source dataset, source mountpoint, base dataset,
	# direction. A literal '-' represents an absent base: Bash read treats tabs as
	# whitespace and would otherwise collapse the empty TSV field, shifting the
	# direction "empty" into base_dataset during the later apply run.
	[[ -n $base_dataset ]] || base_dataset=-
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$client" "$backup" "$source_dataset" "$source_mountpoint" "$base_dataset" "$direction" >> "$client_plan"
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$client" "$backup" "$source_dataset" "$source_mountpoint" "$base_dataset" "$direction" >> "$repair_plan_file"
}

build_client_plan()
{
	local client=$1
	local client_plan="$state_root/repair-plan.$client.tsv"
	local -a backups=() datasets=() mountpoints=() bad=()
	local line backup dataset mountpoint i start end base_index direction

	: > "$client_plan"
	while IFS=$'\t' read -r _client backup dataset mountpoint; do
		[[ $_client == "$client" ]] || continue
		backups+=("$backup")
		datasets+=("$dataset")
		mountpoints+=("$mountpoint")
		if is_bad_dataset "$dataset" "$client"; then
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

remove_inplace_io_error_targets()
{
	local source_mountpoint=$1 replacement_mountpoint=$2 copy_log=$3
	local error_line error_path relative_path replacement_path
	local unmapped_count=0
	local -a replacement_paths=()
	local -A replacement_paths_seen=()
	# Keep the ERE in a variable so Bash does not reinterpret its literal
	# parentheses while parsing [[ ... =~ ... ]]. The path is capture group one.
	local rsync_eio_regex='^rsync: \[[^][]+\] .*\("([^"]*)"\) failed: Input/output error \(5\)$'

	while IFS= read -r error_line; do
		[[ $error_line == *'failed: Input/output error (5)' ]] || continue
		if [[ $error_line =~ $rsync_eio_regex ]]; then
			error_path=${BASH_REMATCH[1]}
		else
			printf 'zfs_metadata_repair: could not map rsync EIO line: %s\n' "$error_line" >> "$copy_log"
			((unmapped_count += 1))
			continue
		fi

		if [[ $error_path != "$source_mountpoint/"* ]]; then
			printf 'zfs_metadata_repair: rsync EIO path is outside the source tree: %s\n' "$error_path" >> "$copy_log"
			((unmapped_count += 1))
			continue
		fi
		relative_path=${error_path#"$source_mountpoint"/}
		if [[ -z $relative_path || /$relative_path/ == *'/../'* ]]; then
			printf 'zfs_metadata_repair: refusing unsafe rsync EIO path: %s\n' "$error_path" >> "$copy_log"
			((unmapped_count += 1))
			continue
		fi

		replacement_path="$replacement_mountpoint/$relative_path"
		if [[ -f $replacement_path && ! -L $replacement_path ]]; then
			# A path can appear in more than one rsync diagnostic. Delete it once so
			# the pre-delete count accurately describes the pending modifications.
			if [[ ! -v "replacement_paths_seen[$replacement_path]" ]]; then
				replacement_paths_seen[$replacement_path]=1
				replacement_paths+=("$replacement_path")
			fi
		elif [[ -e $replacement_path || -L $replacement_path ]]; then
			# Directories and links are not partially overwritten by rsync --inplace.
			# They may be valid content inherited from the clean clone, so retain them
			# when the source failed before rsync could safely inspect that object.
			printf 'zfs_metadata_repair: retained non-regular target after source EIO: %s\n' "$replacement_path" >> "$copy_log"
		fi
	done < "$copy_log"

	if (( unmapped_count != 0 )); then
		warn "Could not safely map $unmapped_count rsync EIO path(s); see $copy_log. No snapshot will be created."
		return 1
	fi
	if ((${#replacement_paths[@]} != 0)); then
		warn "Post-processing will delete ${#replacement_paths[@]} target file(s) affected by rsync source EIO; details: $copy_log"
		for replacement_path in "${replacement_paths[@]}"; do
			# --inplace may have changed the inherited regular file before rsync
			# discovers the source EIO. Removing that file avoids preserving a
			# partially overwritten object in the later @ro snapshot.
			rm -f -- "$replacement_path" || die "Could not remove target affected by rsync EIO: $replacement_path"
			printf 'zfs_metadata_repair: removed inplace target after source EIO: %s\n' "$replacement_path" >> "$copy_log"
		done
	fi
}

copy_dataset_tree()
{
	local source_mountpoint=$1 replacement_mountpoint=$2 copy_log=$3
	local rsync_rc=0
	local -a copy_pipeline_status=()
	local -a rsync_options=(-aHAXS --numeric-ids --ignore-errors --info=progress2 --no-inc-recursive)

	log "Copying \"$source_mountpoint\" to \"$replacement_mountpoint\""
	if (( rsync_inplace )); then
		# Keep the dry-run command identical to apply mode so its review output
		# accurately shows the requested overwrite behaviour.
		rsync_options+=(--inplace)
	fi
	if (( apply )); then
		# --delete is deliberately absent: source EIO must not make an unreadable
		# source directory look empty. --inplace remains opt-in because it can
		# partially overwrite an inherited good file if the source read fails.
		# progress2 reports the complete rsync transfer on stdout. Expected source
		# EIO diagnostics stay on stderr and are recorded only in the per-backup
		# log, so a damaged metadata tree does not make the terminal unusable.
		: > "$copy_log"
		set +e
		LC_ALL=C rsync "${rsync_options[@]}" -- "$source_mountpoint/" "$replacement_mountpoint/" 2>>"$copy_log" | tee -a "$copy_log"
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
		if (( rsync_inplace )); then
			remove_inplace_io_error_targets "$source_mountpoint" "$replacement_mountpoint" "$copy_log" || die "Could not safely remove every target affected by rsync EIO"
		fi
		if (( rsync_rc != 0 )); then
			warn "rsync returned $rsync_rc for \"$source_mountpoint\"; unreadable entries were not copied. Details: $copy_log"
		fi
	else
		log "DRY-RUN: $(printf '%q ' rsync "${rsync_options[@]}" -- "$source_mountpoint/" "$replacement_mountpoint/")"
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

resolve_replacement_dataset()
{
	local source_dataset=$1 candidate
	# Check current run_id first
	if dataset_exists "$source_dataset.repair.$run_id"; then
		printf '%s\n' "$source_dataset.repair.$run_id"
		return 0
	fi
	# Check any existing completed .repair.* dataset with @ro snapshot
	while IFS= read -r candidate; do
		if dataset_snapshot_exists "$candidate"; then
			printf '%s\n' "$candidate"
			return 0
		fi
	done < <(zfs list -H -o name -r "${source_dataset%/*}" 2>/dev/null | grep -E "^${source_dataset}\.repair\." || true)

	printf '%s.repair.%s\n' "$source_dataset" "$run_id"
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
		replacement_dataset=$(resolve_replacement_dataset "$source_dataset")
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
		replacement_dataset=$(resolve_replacement_dataset "$source_dataset")
		archived_dataset="$source_dataset.__pre_metadata_repair_$run_id"

		# Each action reconciles its own observable state. This keeps a retry safe
		# after a busy mount prevented a prior swap from reaching the next action.
		archive_old_dataset "$source_dataset" "$archived_dataset"

		# Rename replacement dataset to the authentic source_dataset name
		if dataset_exists "$replacement_dataset"; then
			if [[ $(zfs get -H -o value mounted "$replacement_dataset") == yes ]]; then
				run zfs unmount "$replacement_dataset"
			fi
			run zfs rename "$replacement_dataset" "$source_dataset"
		fi

		activate_replacement_dataset "$source_dataset" "$source_mountpoint"
	done < "$client_plan"
}

is_client_scan_cached()
{
	local client=$1
	(( force_rescan == 0 )) || return 1
	local client_done="$state_root/scan-status.$client.done"
	local client_bad="$state_root/bad-datasets.$client.tsv"
	local client_completed="$state_root/client.$client.completed"

	if [[ -f $client_completed ]]; then
		return 0
	fi
	if [[ -f $client_done ]]; then
		return 0
	fi
	return 1
}

scan_single_client()
{
	local client=$1
	local client_bad="$state_root/bad-datasets.$client.tsv"
	local client_errors="$state_root/scan-errors.$client.log"
	local client_unreadable="$state_root/unreadable-paths.$client.tsv"
	local client_done="$state_root/scan-status.$client.done"
	local client_completed="$state_root/client.$client.completed"

	if is_client_scan_cached "$client"; then
		if [[ -f $client_completed ]] || [[ -f $client_done && ! -s $client_bad ]]; then
			log "Client \"$client\": already verified/completed (cached clean). Skipping scan."
			: > "$client_bad"
		else
			log "Client \"$client\": reusing cached scan result ($(wc -l < "$client_bad" 2>/dev/null || echo 0) affected dataset(s))."
		fi
		return 0
	fi

	rm -f "$client_done" "$client_completed"
	if (( force_rescan )); then
		local _c _b _d _m _dk
		while IFS=$'\t' read -r _c _b _d _m; do
			[[ $_c == "$client" ]] || continue
			_dk=$(printf '%s' "$_d" | tr '/ ' '__')
			rm -f "$state_root/scan-dataset.$_dk.done" "$state_root/scan-status.$_dk.log"
		done < "$all_datasets_file"
	fi
	local tmp_bad="$client_bad.tmp.$$"
	local tmp_errors="$client_errors.tmp.$$"
	local tmp_unreadable="$client_unreadable.tmp.$$"
	: > "$tmp_bad"
	: > "$tmp_errors"
	: > "$tmp_unreadable"

	log "Scanning metadata tree of client \"$client\"..."
	local _client backup dataset mountpoint
	while IFS=$'\t' read -r _client backup dataset mountpoint; do
		[[ $_client == "$client" ]] || continue
		scan_dataset "$dataset" "$mountpoint" "$tmp_bad" "$tmp_errors" "$tmp_unreadable"
		abort_if_interrupted
	done < "$all_datasets_file"

	mv -f "$tmp_bad" "$client_bad"
	mv -f "$tmp_errors" "$client_errors"
	mv -f "$tmp_unreadable" "$client_unreadable"
	touch "$client_done"
}

perform_metadata_scan()
{
	collect_datasets
	if [[ ! -s $all_datasets_file ]]; then
		warn "No matching backup datasets found below $dataset_parent"
		exit 0
	fi

	local client
	while IFS=$'\t' read -r client; do
		abort_if_interrupted
		scan_single_client "$client"
	done < <(cut -f1 "$all_datasets_file" | sort -u)

	# Aggregate global state files for the current selection
	: > "$bad_datasets_file"
	: > "$scan_errors_file"
	: > "$unreadable_paths_file"

	while IFS=$'\t' read -r client; do
		local client_bad="$state_root/bad-datasets.$client.tsv"
		local client_errors="$state_root/scan-errors.$client.log"
		local client_unreadable="$state_root/unreadable-paths.$client.tsv"
		[[ -s $client_bad ]] && cat "$client_bad" >> "$bad_datasets_file"
		[[ -s $client_errors ]] && cat "$client_errors" >> "$scan_errors_file"
		[[ -s $client_unreadable ]] && cat "$client_unreadable" >> "$unreadable_paths_file"
	done < <(cut -f1 "$all_datasets_file" | sort -u)

	write_scan_context
}

perform_metadata_scan

if (( resuming_apply_run == 0 )) || [[ ! -s $repair_plan_file ]]; then
	: > "$repair_plan_file"
	while IFS=$'\t' read -r client; do
		build_client_plan "$client"
	done < <(cut -f1 "$all_datasets_file" | sort -u)
else
	log "Resuming with existing repair plan: $repair_plan_file ($(wc -l < "$repair_plan_file") dataset(s))"
fi

if [[ ! -s $bad_datasets_file ]]; then
	log "No metadata traversal errors found for requested client(s). State directory: $state_root"
	exit 0
fi

log "Affected datasets: $(wc -l < "$bad_datasets_file")"
log "Repair plan: $repair_plan_file"
log "Scan errors: $scan_errors_file"
log "Unreadable path indications: $unreadable_paths_file"
if (( apply == 0 || scan_only )); then
	log "Dry-run complete. Review the plan and rerun with --apply to create replacement datasets."
	exit 0
fi

delete_client_archived_datasets()
{
	local client=$1 client_plan="$state_root/repair-plan.$client.tsv"
	local plan_client backup source_dataset source_mountpoint base_dataset direction
	local -a client_archived=()

	[[ -s $client_plan ]] || return 0
	log "Deleting archived old datasets for client \"$client\"..."
	while IFS=$'\t' read -r plan_client backup source_dataset source_mountpoint base_dataset direction; do
		client_archived+=("$source_dataset.__pre_metadata_repair_$run_id")
	done < "$client_plan"

	delete_archived_repair_datasets "${client_archived[@]}"
}

while IFS=$'\t' read -r client; do
	abort_if_interrupted
	log "=== Processing client: $client ==="
	rebuild_client "$client"
	swap_client_datasets "$client"
	if (( delete_archived_repairs )); then
		delete_client_archived_datasets "$client"
	fi
	# Mark client completed and truncate its bad-datasets file
	: > "$state_root/bad-datasets.$client.tsv"
	rm -f "$state_root/repair-plan.$client.tsv"
	touch "$state_root/client.$client.completed"
	touch "$state_root/scan-status.$client.done"
	log "Client \"$client\" repair & migration completed successfully."
done < <(cut -f1 "$repair_plan_file" | sort -u)

log "Migration complete. Replacement datasets are active as UrBackup linear clone chain."
if (( apply )); then
	rm -f "$active_run_id_file"
fi
log "Archived dataset mountpoint properties: $archived_properties_file"
log "Next step after verifying mounts and data: urbackupsrv reconcile-storage --remove-missing-files --drop-empty-backups --apply"
