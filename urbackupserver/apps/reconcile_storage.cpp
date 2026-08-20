/*************************************************************************
*    UrBackup - Client/Server backup system
*    Copyright (C) 2011-2026 Martin Raiber
*
*    This program is free software: you can redistribute it and/or modify
*    it under the terms of the GNU Affero General Public License as published by
*    the Free Software Foundation, either version 3 of the License, or
*    (at your option) any later version.
**************************************************************************/

#include "reconcile_storage.h"
#include "app.h"
#include "../snapshot_helper.h"
#include "../server.h"
#include "../server_hash.h"
#include "../server_settings.h"
#include "../create_files_index.h"
#include "../serverinterface/helper.h"
#include "../../Interface/DatabaseCursor.h"
#include "../../Interface/File.h"
#include "../../urbackupcommon/chunk_hasher.h"
#include "../../urbackupcommon/file_metadata.h"
#include "../../urbackupcommon/os_functions.h"
#include "../../urbackupcommon/TreeHash.h"
#include "../../stringtools.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <signal.h>
#include <string>
#include <vector>
#ifndef _WIN32
#include <unistd.h>
#endif

namespace
{
struct SColumn
{
	std::string name;
	std::string type;
};

struct SStats
{
	SStats()
		: old_backups_seen(0), missing_backups_importable(0), missing_backups_imported(0),
		missing_backups_skipped(0), file_rows_importable(0), file_rows_imported(0),
		file_rows_reused(0), file_rows_skipped_malformed(0),
		dataset_links(0), dataset_links_created(0), dataset_backups_seen(0),
		dataset_backups_importable(0), dataset_backups_imported(0), ignored_read_errors(0), broken_storage_link_targets(0),
		paths_to_migrate(0), paths_migrated(0), missing_files(0), missing_files_removed(0),
		malformed_file_rows(0), malformed_file_rows_removed(0), empty_backups(0), empty_backups_removed(0)
	{
	}

	int64 old_backups_seen;
	int64 missing_backups_importable;
	int64 missing_backups_imported;
	int64 missing_backups_skipped;
	int64 file_rows_importable;
	int64 file_rows_imported;
	int64 file_rows_reused;
	int64 file_rows_skipped_malformed;
	int64 dataset_links;
	int64 dataset_links_created;
	int64 dataset_backups_seen;
	int64 dataset_backups_importable;
	int64 dataset_backups_imported;
	int64 ignored_read_errors;
	int64 broken_storage_link_targets;
	int64 paths_to_migrate;
	int64 paths_migrated;
	int64 missing_files;
	int64 missing_files_removed;
	int64 malformed_file_rows;
	int64 malformed_file_rows_removed;
	int64 empty_backups;
	int64 empty_backups_removed;
};

struct SCopyProgress
{
	SCopyProgress()
		: total_bytes(0), copied_bytes(0), last_progress_bytes(0), last_log_time(0)
	{
	}

	_i64 total_bytes;
	_i64 copied_bytes;
	_i64 last_progress_bytes;
	int64 last_log_time;
};

struct SCursorProgress
{
	SCursorProgress(const std::string& label, _i64 total)
		: label(label), total(total), current(0), last_rate_current(0),
		last_rate_time(0), last_draw_time(0), rows_per_second(0),
		max_line_length(0), drawn(false)
	{
	}

	std::string label;
	std::string detail;
	std::string status_message;
	_i64 total;
	_i64 current;
	_i64 last_rate_current;
	int64 last_rate_time;
	int64 last_draw_time;
	int64 rows_per_second;
	size_t max_line_length;
	bool drawn;
};

struct STableColumnInfo
{
	std::string name;
	std::string type;
	std::string default_value;
	bool notnull;
};

struct SDatasetEntry
{
	bool image;
	std::string dataset_name;
	std::string mountpoint;
	std::string clientname;
	std::string backup_path;
};

struct SStorageBackupEntry
{
	bool image;
	bool symlink;
	bool standalone_image_file;
	bool client_exists_in_db;
	std::string root_path;
	std::string payload_path;
	std::string clientname;
	std::string backup_path;
};

struct SImportedDatasetBackup
{
	SImportedDatasetBackup()
		: clientid(0), backupid(0), incremental(0), total_size(0), file_rows(0),
		resume_existing_backup(false)
	{
	}

	int64 clientid;
	int64 backupid;
	int incremental;
	std::string clientname;
	std::string backup_path;
	std::string sqlite_time;
	_i64 total_size;
	int64 file_rows;
	bool resume_existing_backup;
};

enum ELookupStatus
{
	ELookupStatus_Error = -1,
	ELookupStatus_NotFound = 0,
	ELookupStatus_Found = 1
};

struct SExistingFileBackupState
{
	SExistingFileBackupState()
		: status(ELookupStatus_NotFound), id(0), complete(false), done(false)
	{
	}

	int status;
	int64 id;
	bool complete;
	bool done;
};

enum ETreeImportResult
{
	ETreeImportResult_Ok,
	ETreeImportResult_Error,
	ETreeImportResult_Stopped
};

volatile sig_atomic_t reconcile_stop_requested = 0;
SCursorProgress* active_cursor_progress = NULL;
SCursorProgress* fileindex_status_progress = NULL;

void set_cursor_progress_status(SCursorProgress& progress, const std::string& msg);

void reconcile_signal_handler(int signum)
{
	reconcile_stop_requested = signum;
}

bool is_reconcile_stop_requested()
{
	return reconcile_stop_requested != 0;
}

void reconcile_fileindex_status_message(const std::string& msg)
{
	if (fileindex_status_progress != NULL)
	{
		set_cursor_progress_status(*fileindex_status_progress, msg);
	}
}

class ScopedFileIndexStatusProgress
{
public:
	ScopedFileIndexStatusProgress(SCursorProgress* progress)
		: previous_progress(fileindex_status_progress)
	{
		fileindex_status_progress = progress;
		set_fileindex_status_message_callback(progress != NULL ? reconcile_fileindex_status_message : NULL);
		FileIndex::set_runtime_status_callback(progress != NULL ? reconcile_fileindex_status_message : NULL);
	}

	~ScopedFileIndexStatusProgress()
	{
		fileindex_status_progress = previous_progress;
		set_fileindex_status_message_callback(previous_progress != NULL ? reconcile_fileindex_status_message : NULL);
		FileIndex::set_runtime_status_callback(previous_progress != NULL ? reconcile_fileindex_status_message : NULL);
	}

private:
	SCursorProgress* previous_progress;
};

class ScopedReconcileSignalHandlers
{
public:
	ScopedReconcileSignalHandlers()
		: active(false), old_sigint(SIG_DFL), old_sigterm(SIG_DFL)
	{
		reconcile_stop_requested = 0;
		old_sigint = signal(SIGINT, reconcile_signal_handler);
		old_sigterm = signal(SIGTERM, reconcile_signal_handler);
		active = true;
	}

	~ScopedReconcileSignalHandlers()
	{
		if (active)
		{
			signal(SIGINT, old_sigint);
			signal(SIGTERM, old_sigterm);
		}
	}

private:
	bool active;
	void (*old_sigint)(int);
	void (*old_sigterm)(int);
};

// The dataset-link reconciliation path reuses the interactive cursor progress helpers
// below, so forward declarations keep the helper order stable and avoid reshuffling
// already debugged output code.
void draw_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail, bool force);
void finish_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail);
void dismiss_cursor_progress(SCursorProgress& progress);
void log_with_cursor_progress(SCursorProgress& progress, const std::string& msg, int loglevel);
int current_client_id(IDatabase* db, const std::string& clientname, int64& clientid);
int current_backup_exists(IDatabase* db, const std::string& clientname, const std::string& path);
SExistingFileBackupState current_file_backup_state(IDatabase* db, const std::string& clientname, const std::string& path);
std::vector<STableColumnInfo> table_column_info(IDatabase* db, const std::string& dbname, const std::string& table);
std::string column_list(const std::vector<SColumn>& columns, const std::string& prefix);
std::string bind_list(size_t n);
bool should_skip_dataset_entry_name(const std::string& name);
std::string metadata_path_from_relative(const std::string& metadata_root, const std::string& relative_path);

std::string strip_trailing_sep(std::string path)
{
	while (path.size() > 1
		&& path.substr(path.size() - os_file_sep().size()) == os_file_sep())
	{
		path.erase(path.size() - os_file_sep().size());
	}
	return path;
}

std::string sql_string(const std::string& value)
{
	std::string ret = "'";
	for (size_t i = 0; i < value.size(); ++i)
	{
		if (value[i] == '\'')
		{
			ret += "''";
		}
		else
		{
			ret += value[i];
		}
	}
	ret += "'";
	return ret;
}

std::string sql_ident(const std::string& value)
{
	std::string ret = "\"";
	for (size_t i = 0; i < value.size(); ++i)
	{
		if (value[i] == '"')
		{
			ret += "\"\"";
		}
		else
		{
			ret += value[i];
		}
	}
	ret += "\"";
	return ret;
}

std::string shell_single_quote(const std::string& value)
{
	std::string ret = "'";
	for (size_t i = 0; i < value.size(); ++i)
	{
		if (value[i] == '\'')
		{
			ret += "'\\''";
		}
		else
		{
			ret += value[i];
		}
	}
	ret += "'";
	return ret;
}

std::set<std::string> parse_client_filter_set(const std::string& client_filter)
{
	std::set<std::string> ret;
	std::vector<std::string> toks;
	Tokenize(client_filter, toks, ",");
	for (size_t i = 0; i < toks.size(); ++i)
	{
		const std::string clientname = trim(toks[i]);
		if (!clientname.empty())
		{
			ret.insert(clientname);
		}
	}
	return ret;
}

bool matches_client_filter(const std::set<std::string>& client_filters, const std::string& clientname)
{
	return client_filters.empty() || client_filters.find(clientname) != client_filters.end();
}

std::string client_filter_sql_expr(const std::string& client_filter, const std::string& qualified_column)
{
	const std::set<std::string> client_filters = parse_client_filter_set(client_filter);
	if (client_filters.empty())
	{
		return std::string();
	}

	if (client_filters.size() == 1)
	{
		return qualified_column + "=" + sql_string(*client_filters.begin());
	}

	std::string ret = qualified_column + " IN (";
	for (std::set<std::string>::const_iterator it = client_filters.begin(); it != client_filters.end(); ++it)
	{
		if (it != client_filters.begin())
		{
			ret += ", ";
		}
		ret += sql_string(*it);
	}
	ret += ")";
	return ret;
}

bool starts_with(const std::string& value, const std::string& prefix)
{
	return value.size() >= prefix.size()
		&& value.substr(0, prefix.size()) == prefix;
}

bool is_absolute_path(const std::string& path)
{
	return !path.empty()
		&& (path[0] == '/'
#ifdef _WIN32
			|| (path.size() > 2 && path[1] == ':')
#endif
			);
}

std::string join_path(const std::string& a, const std::string& b)
{
	if (a.empty())
	{
		return b;
	}
	if (b.empty())
	{
		return a;
	}
	if (a.substr(a.size() - os_file_sep().size()) == os_file_sep())
	{
		return a + b;
	}
	return a + os_file_sep() + b;
}

// Storage imports need to address the on-disk .hashes tree exactly like the normal
// backup code does. Escaping every path component here keeps reconcile-storage
// compatible with metadata files whose names would otherwise collide with special
// markers such as ".dir_metadata".
std::string metadata_path_from_relative(const std::string& metadata_root, const std::string& relative_path)
{
	if (relative_path.empty())
	{
		return metadata_root;
	}

	std::vector<std::string> parts;
	Tokenize(relative_path, parts, os_file_sep());

	std::string ret = metadata_root;
	for (size_t i = 0; i < parts.size(); ++i)
	{
		ret = join_path(ret, escape_metadata_fn(parts[i]));
	}

	return ret;
}

bool path_exists(const std::string& path)
{
	if (path.empty())
	{
		return false;
	}
	const std::string prefixed = os_file_prefix(path);
	return FileExists(prefixed) || os_directory_exists(prefixed);
}

bool file_index_exists()
{
	return FileExists("urbackup/fileindex/backup_server_files_index.lmdb");
}

bool ensure_file_index_available_for_reconcile(bool force_rebuild)
{
	// Reconcile-storage can run without the file index for many read-only or
	// metadata-only actions. Storage-backed file imports, however, call into the
	// normal hash/link pipeline and therefore need a current LMDB file index.
	if (!force_rebuild && file_index_exists())
	{
		return true;
	}

	if (force_rebuild)
	{
		Server->Log("Rebuilding file index for reconcile-storage...", LL_INFO);
		delete_file_index();
	}
	else
	{
		Server->Log("File index is missing. Creating it before reconcile-storage continues...", LL_WARNING);
	}

	SStartupStatus startup_status;
	return create_files_index(startup_status);
}

std::string row_result_value(const db_single_result& row, const std::string& key, const std::string& fallback = std::string())
{
	db_single_result::const_iterator it = row.find(key);
	if (it == row.end())
	{
		return fallback;
	}
	return it->second;
}

// Reconcile-storage creates the same visible symlink objects as the normal server mode:
// a temporary link is created first and then renamed atomically so readers never observe
// a half-written final path. Existing filesystem objects are intentionally left untouched.
bool create_dataset_link_at_path(const std::string& mountpoint, const std::string& link_path, bool apply)
{
	const std::string tmp_path = link_path + ".reconcile-storage-new";

	if (os_get_file_type(os_file_prefix(link_path)) != 0)
	{
		return true;
	}

	if (apply)
	{
		Server->deleteFile(os_file_prefix(tmp_path));

		const std::string parent_dir = ExtractFilePath(link_path);
		if (!parent_dir.empty() && !os_directory_exists(os_file_prefix(parent_dir)))
		{
			if (!os_create_dir_recursive(os_file_prefix(parent_dir)))
			{
				Server->Log("Could not create parent directory \"" + parent_dir + "\" for dataset link. "
					+ os_last_error_str(), LL_ERROR);
				return false;
			}
		}

		if (!os_link_symbolic(mountpoint, tmp_path))
		{
			Server->Log("Could not create temporary dataset link at \"" + tmp_path + "\" to \"" + mountpoint
				+ "\". " + os_last_error_str(), LL_ERROR);
			return false;
		}

		if (!os_rename_file(tmp_path, link_path))
		{
			Server->Log("Could not rename temporary dataset link from \"" + tmp_path + "\" to \"" + link_path
				+ "\". " + os_last_error_str(), LL_ERROR);
			Server->deleteFile(os_file_prefix(tmp_path));
			return false;
		}
	}

	return true;
}

bool should_recreate_image_dataset_link(const db_single_result& row)
{
	const std::string image_path = row_result_value(row, "image_path");
	if (image_path.empty())
	{
		return false;
	}

	// Only raw image backups are represented by a symlinked dataset directory in normal
	// server mode. VHD/VHDZ/VHDX variants remain regular files and must not be recreated
	// as directory links here.
	return findextension(image_path) == "raw";
}

std::string read_trimmed_file_if_exists(const std::string& path)
{
	if (!FileExists(os_file_prefix(path)))
	{
		return std::string();
	}

	return trim(getFile(path));
}

void warn_if_dataset_detection_failed()
{
	const std::string dataset_image = read_trimmed_file_if_exists("/etc/urbackup/dataset");
	const std::string dataset_file = read_trimmed_file_if_exists("/etc/urbackup/dataset_file");
	if (dataset_image.empty() && dataset_file.empty())
	{
		return;
	}

	if (BackupServer::getSnapshotMethod(false) == BackupServer::ESnapshotMethod_ZfsFile
		|| BackupServer::getSnapshotMethod(true) == BackupServer::ESnapshotMethod_Zfs)
	{
		return;
	}

	std::string configured_roots;
	if (!dataset_image.empty())
	{
		configured_roots = "dataset=\"" + dataset_image + "\"";
	}
	if (!dataset_file.empty())
	{
		if (!configured_roots.empty())
		{
			configured_roots += " ";
		}
		configured_roots += "dataset_file=\"" + dataset_file + "\"";
	}

	std::string msg = "Dataset link recreation is enabled and dataset roots are configured (" + configured_roots
		+ "), but snapshot detection did not activate ZFS dataset-backed backups. Reconcile-storage therefore cannot determine dataset mountpoints or link targets.";
#ifndef _WIN32
	// The snapshot helper test is often privilege-sensitive on Linux/ZFS setups. A
	// non-root reconcile run should call this out explicitly so operators can retry
	// with the same privileges as the regular server process.
	if (geteuid() != 0)
	{
		msg += " The current process is not running as root; if the snapshot helper requires root privileges for ZFS detection, rerun reconcile-storage with root privileges or the same user the working server uses.";
	}
#endif
	Server->Log(msg, LL_WARNING);
}

#ifndef _WIN32
std::string find_zfs_cmd()
{
	const char* const locations[] = { "zfs", "/sbin/zfs", "/bin/zfs", "/usr/sbin/zfs", "/usr/bin/zfs" };
	for (size_t i = 0; i < sizeof(locations) / sizeof(locations[0]); ++i)
	{
		if (system((std::string(locations[i]) + " list >/dev/null 2>&1").c_str()) == 0)
		{
			return locations[i];
		}
	}
	return "zfs";
}
#endif

bool list_zfs_dataset_entries(const std::string& dataset_root, bool image, std::vector<SDatasetEntry>& entries)
{
#ifdef _WIN32
	(void)dataset_root;
	(void)image;
	(void)entries;
	return false;
#else
	if (dataset_root.empty())
	{
		return true;
	}

	std::string output;
	const std::string zfs_cmd = find_zfs_cmd();
	const int rc = os_popen(zfs_cmd + " list -H -o name,mountpoint -r " + shell_single_quote(dataset_root), output);
	if (rc != 0)
	{
		Server->Log("Could not enumerate ZFS datasets below \"" + dataset_root + "\" using \"" + zfs_cmd + "\"", LL_ERROR);
		return false;
	}

	std::vector<std::string> lines;
	Tokenize(output, lines, "\n");
	for (size_t i = 0; i < lines.size(); ++i)
	{
		std::vector<std::string> cols;
		Tokenize(lines[i], cols, "\t");
		if (cols.size() < 2)
		{
			continue;
		}

		const std::string dataset_name = trim(cols[0]);
		const std::string mountpoint = trim(cols[1]);
		if (dataset_name == dataset_root)
		{
			continue;
		}
		if (!starts_with(dataset_name, dataset_root + "/"))
		{
			continue;
		}
		if (mountpoint.empty() || mountpoint == "-")
		{
			continue;
		}

		const std::string relative = dataset_name.substr(dataset_root.size() + 1);
		std::vector<std::string> parts;
		Tokenize(relative, parts, "/");
		if (parts.size() < 2)
		{
			continue;
		}

		SDatasetEntry entry;
		entry.image = image;
		entry.dataset_name = dataset_name;
		entry.mountpoint = mountpoint;
		entry.clientname = parts[0];
		entry.backup_path = parts[1];
		for (size_t j = 2; j < parts.size(); ++j)
		{
			entry.backup_path += "/" + parts[j];
		}
		entries.push_back(entry);
	}

	return true;
#endif
}

bool list_all_dataset_entries(std::vector<SDatasetEntry>& entries)
{
	const std::string dataset_image = read_trimmed_file_if_exists("/etc/urbackup/dataset");
	const std::string dataset_file = read_trimmed_file_if_exists("/etc/urbackup/dataset_file");
	return list_zfs_dataset_entries(dataset_file, false, entries)
		&& list_zfs_dataset_entries(dataset_image, true, entries);
}

bool parse_backup_time_from_path(const std::string& backup_path, std::string& sqlite_time)
{
	if (backup_path.size() < 11)
	{
		return false;
	}

	const std::string stamp = backup_path.substr(0, 11);
	if (stamp[6] != '-')
	{
		return false;
	}

	tm t;
	memset(&t, 0, sizeof(t));
	t.tm_year = 100 + watoi(stamp.substr(0, 2));
	t.tm_mon = watoi(stamp.substr(2, 2)) - 1;
	t.tm_mday = watoi(stamp.substr(4, 2));
	t.tm_hour = watoi(stamp.substr(7, 2));
	t.tm_min = watoi(stamp.substr(9, 2));
	t.tm_sec = 0;

	char buf[32];
	if (strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &t) == 0)
	{
		return false;
	}
	sqlite_time = buf;
	return true;
}

int current_image_backup_exists(IDatabase* db, const std::string& clientname, const std::string& backup_path)
{
	IQuery* q = db->Prepare("SELECT i.id FROM backup_images i INNER JOIN clients c ON c.id=i.clientid WHERE c.name=? AND i.path LIKE ?", false);
	if (q == NULL)
	{
		// Storage reconciliation must not continue with a failed lookup query. If
		// Prepare() already reported a schema/connection error, treating that as
		// "backup not found" would import duplicates, while dereferencing NULL
		// would crash immediately.
		return ELookupStatus_Error;
	}
	q->Bind(clientname);
	q->Bind("%/" + clientname + "/" + backup_path + "/%");
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return res.empty() ? ELookupStatus_NotFound : ELookupStatus_Found;
}

int64 ensure_client(IDatabase* db, const std::string& clientname, bool apply)
{
	int64 clientid = 0;
	const int lookup_status = current_client_id(db, clientname, clientid);
	if (lookup_status == ELookupStatus_Found)
	{
		return clientid;
	}
	if (lookup_status == ELookupStatus_Error)
	{
		return 0;
	}

	if (!apply)
	{
		return -1;
	}

	// Missing clients are reconstructed minimally so later backup/file imports can
	// attach to a consistent client id. Remaining statistics are rebuilt from the
	// imported backup rows and files rows below.
	IQuery* q = db->Prepare("INSERT INTO clients(name) VALUES(?)", false);
	q->Bind(clientname);
	if (!q->Write())
	{
		q->Reset();
		db->destroyQuery(q);
		return 0;
	}
	q->Reset();
	db->destroyQuery(q);
	return db->getLastInsertID();
}

std::vector<STableColumnInfo> subset_column_info(const std::vector<STableColumnInfo>& cols, const std::set<std::string>& selected)
{
	std::vector<STableColumnInfo> ret;
	for (size_t i = 0; i < cols.size(); ++i)
	{
		if (selected.find(cols[i].name) != selected.end())
		{
			ret.push_back(cols[i]);
		}
	}
	return ret;
}

bool insert_named_row(IDatabase* db, const std::string& table, const std::vector<STableColumnInfo>& columns,
	const std::map<std::string, std::string>& values)
{
	std::vector<SColumn> query_columns;
	for (size_t i = 0; i < columns.size(); ++i)
	{
		if (values.find(columns[i].name) != values.end())
		{
			SColumn col;
			col.name = columns[i].name;
			col.type = columns[i].type;
			query_columns.push_back(col);
		}
	}
	if (query_columns.empty())
	{
		Server->Log("No column values prepared for insert into table \"" + table + "\"", LL_ERROR);
		return false;
	}

	IQuery* q = db->Prepare("INSERT INTO " + table + " (" + column_list(query_columns, "")
		+ ") VALUES (" + bind_list(query_columns.size()) + ")", false);
	for (size_t i = 0; i < query_columns.size(); ++i)
	{
		std::map<std::string, std::string>::const_iterator it = values.find(query_columns[i].name);
		if (query_columns[i].name == "shahash")
		{
			q->Bind(it->second.c_str(), static_cast<_u32>(it->second.size()));
		}
		else
		{
			q->Bind(it->second);
		}
	}
	const bool ok = q->Write();
	q->Reset();
	db->destroyQuery(q);
	return ok;
}

bool update_client_rollups(IDatabase* db, int64 clientid)
{
	std::string clientid_str = convert(clientid);
	// Storage imports already know the owning backup rows. Summing via backups.id ->
	// files.backupid lets SQLite use the files_backupid index, whereas filtering
	// directly on files.clientid caused large full scans over backup_server_files.db
	// and made the post-import rollup phase appear to hang on big installations.
	return db->Write("UPDATE clients SET "
		"bytes_used_files=COALESCE((SELECT SUM(f.rsize) FROM curr_files.files f "
			"INNER JOIN backups b ON b.id=f.backupid WHERE b.clientid=" + clientid_str + "),0), "
		"bytes_used_images=COALESCE((SELECT SUM(size_bytes) FROM backup_images WHERE clientid=" + clientid_str + "),0), "
		"lastbackup=(SELECT MAX(backuptime) FROM backups WHERE clientid=" + clientid_str + "), "
		"lastbackup_image=(SELECT MAX(backuptime) FROM backup_images WHERE clientid=" + clientid_str + ") "
		"WHERE id=" + clientid_str);
}

std::string dataset_link_path(const std::string& storage_prefix, const SDatasetEntry& entry)
{
	return join_path(join_path(storage_prefix, entry.clientname), entry.backup_path);
}

bool is_image_backup_name(const std::string& backup_path)
{
	return backup_path.find("_Image_") != std::string::npos;
}

bool is_supported_image_extension(const std::string& ext)
{
	return ext == "raw" || ext == "vhd" || ext == "vhdz" || ext == "vhdx" || ext == "vhdxz";
}

std::string strip_file_extension(const std::string& filename)
{
	const std::string ext = findextension(filename);
	if (ext.empty() || filename.size() <= ext.size() + 1)
	{
		return filename;
	}

	return filename.substr(0, filename.size() - ext.size() - 1);
}

bool is_backup_timestamp_component(const std::string& backup_path)
{
	if (backup_path.size() < 11 || backup_path[6] != '-')
	{
		return false;
	}

	// Visible storage imports must only consider objects that match the regular
	// UrBackup naming scheme. This filters out administrative directories and
	// copied helper content before any client/backup reconstruction logic starts.
	for (size_t i = 0; i < 6; ++i)
	{
		if (backup_path[i] < '0' || backup_path[i] > '9')
		{
			return false;
		}
	}
	for (size_t i = 7; i < 11; ++i)
	{
		if (backup_path[i] < '0' || backup_path[i] > '9')
		{
			return false;
		}
	}

	return true;
}

bool is_valid_visible_backup_name(const std::string& backup_path)
{
	if (!is_backup_timestamp_component(backup_path))
	{
		return false;
	}

	if (backup_path.size() == 11)
	{
		return true;
	}

	const std::string image_marker = "_Image_";
	return backup_path.size() > 11 + image_marker.size()
		&& backup_path.substr(11, image_marker.size()) == image_marker;
}

std::string storage_entry_display_name(const SStorageBackupEntry& entry)
{
	if (entry.standalone_image_file)
	{
		return entry.backup_path + "." + findextension(entry.payload_path);
	}

	return entry.backup_path;
}

int current_storage_image_backup_exists(IDatabase* db, const SStorageBackupEntry& entry)
{
	IQuery* q = db->Prepare("SELECT i.id FROM backup_images i INNER JOIN clients c ON c.id=i.clientid "
		"WHERE c.name=? AND i.path LIKE ?", false);
	if (q == NULL)
	{
		// Storage-image imports use the same existence probe as file imports. A
		// failed Prepare() must stop the caller instead of turning into a NULL
		// dereference or a false "missing backup" result.
		return ELookupStatus_Error;
	}

	if (entry.standalone_image_file)
	{
		// Standalone image files are matched by their basename because they do not
		// have the extra backup directory level that directory-backed image backups use.
		q->Bind(entry.clientname);
		q->Bind("%/" + entry.clientname + "/" + ExtractFileName(entry.payload_path));
	}
	else
	{
		q->Bind(entry.clientname);
		q->Bind("%/" + entry.clientname + "/" + entry.backup_path + "/%");
	}
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return res.empty() ? ELookupStatus_NotFound : ELookupStatus_Found;
}

std::string storage_import_action_label(const SStorageBackupEntry& entry)
{
	// Backups belonging to clients that already exist in the database are logged
	// with the same wording remove-unknown implicitly compares against. Storage-only
	// clients remain importable, but are called out separately so both dry-run sets
	// can be compared without those additional reconstruction candidates.
	if (entry.client_exists_in_db)
	{
		return "storage-backed";
	}

	return "storage-only-client";
}

bool should_skip_storage_visible_entry_name(const std::string& name)
{
	// Hidden bookkeeping directories below the storage root are not client backups.
	// Importing them would create bogus client/backup rows such as ".migration-state"
	// and can then trip later assumptions in the reconstruction path.
	return !name.empty() && name[0] == '.';
}

bool list_visible_storage_backup_entries(const std::string& storage_prefix, const std::string& client_filter,
	std::vector<SStorageBackupEntry>& entries, SStats& stats)
{
	const std::set<std::string> client_filters = parse_client_filter_set(client_filter);
	bool has_error = false;
	std::vector<SFile> clients = getFiles(storage_prefix, &has_error);
	if (has_error)
	{
		return false;
	}

	std::sort(clients.begin(), clients.end());
	for (size_t i = 0; i < clients.size(); ++i)
	{
		// Linux marks symlinks from getFiles() as "special" as well. Visible storage
		// backups are frequently represented exactly by those symlinks, so only truly
		// unsupported special files are skipped here while symlink-based clients stay visible.
		if ((!clients[i].isdir && !clients[i].issym) || (clients[i].isspecialf && !clients[i].issym))
		{
			continue;
		}
		if (should_skip_storage_visible_entry_name(clients[i].name))
		{
			continue;
		}
		if (!matches_client_filter(client_filters, clients[i].name))
		{
			continue;
		}

		const std::string client_root = join_path(storage_prefix, clients[i].name);
		bool backup_error = false;
		std::vector<SFile> backups = getFiles(client_root, &backup_error);
		if (backup_error)
		{
			return false;
		}

		std::sort(backups.begin(), backups.end());
		for (size_t j = 0; j < backups.size(); ++j)
		{
			// Backup links use the same Linux SFile flags as other symlinks. Skipping
			// all isspecialf entries hid the majority of dataset-backed backups from
			// import-storage, while remove-unknown still considered them present.
			if (backups[j].isspecialf && !backups[j].issym)
			{
				continue;
			}
			if (should_skip_storage_visible_entry_name(backups[j].name))
			{
				continue;
			}
			SStorageBackupEntry entry;
			entry.image = false;
			entry.symlink = backups[j].issym;
			entry.standalone_image_file = false;
			entry.client_exists_in_db = false;
			entry.root_path = join_path(client_root, backups[j].name);
			entry.payload_path.clear();
			entry.clientname = clients[i].name;

			// Remove-unknown treats plain names without dots as file-backup candidates
			// even when they are not directories. Import-storage mirrors that behavior
			// so copied file backups and snapshot-link style entries are not skipped.
			if (backups[j].isdir || backups[j].issym || backups[j].name.find(".") == std::string::npos)
			{
				if (!is_valid_visible_backup_name(backups[j].name)
					|| should_skip_dataset_entry_name(backups[j].name))
				{
					continue;
				}

				entry.image = is_image_backup_name(backups[j].name);
				entry.backup_path = backups[j].name;
			}
			else
			{
				const std::string ext = findextension(backups[j].name);
				if (!is_supported_image_extension(ext))
				{
					continue;
				}

				const std::string backup_stem = strip_file_extension(backups[j].name);
				if (!is_valid_visible_backup_name(backup_stem) || !is_image_backup_name(backup_stem)
					|| should_skip_dataset_entry_name(backup_stem))
				{
					continue;
				}

				// Standalone image files live directly below the client directory. They
				// are valid UrBackup objects and remove-unknown already considers them.
				entry.image = true;
				entry.standalone_image_file = true;
				entry.backup_path = backup_stem;
				entry.payload_path = entry.root_path;
			}

			// A visible backup symlink whose target is currently not reachable would be
			// seen by later cleanup logic as a broken storage object. Reconcile-storage
			// warns immediately so operators do not run remove-unknown against an
			// incomplete mount state and accidentally delete still-valid backup data.
			if (entry.symlink && !os_directory_exists(os_file_prefix(entry.root_path)))
			{
				++stats.broken_storage_link_targets;
				Server->Log("Visible backup link \"" + entry.root_path + "\" exists, but its target is currently not reachable. "
					"Do not run remove-unknown before the underlying dataset or mount target is available.", LL_WARNING);
			}

			entries.push_back(entry);
		}
	}

	return true;
}

bool insert_dataset_image_backup_row(IDatabase* db, const std::vector<STableColumnInfo>& image_columns,
	int64 clientid, const std::string& sqlite_time, const std::string& image_path,
	const std::string& image_letter, int incremental, _i64 size_bytes, bool apply)
{
	if (!apply)
	{
		return true;
	}

	// Image-backup reconstruction keeps all currently relevant state explicit so the
	// imported row behaves like a completed backup and does not get mistaken for a
	// half-finished or archived job on next startup.
	std::map<std::string, std::string> values;
	values["clientid"] = convert(clientid);
	values["backuptime"] = sqlite_time;
	values["incremental"] = convert(incremental);
	values["incremental_ref"] = "0";
	values["path"] = image_path;
	values["complete"] = "1";
	values["running"] = "1970-01-01 00:00:00";
	values["size_bytes"] = convert(size_bytes);
	values["done"] = "1";
	values["version"] = "0";
	values["letter"] = image_letter;
	values["archived"] = "0";
	values["archive_timeout"] = "0";
	values["mounttime"] = "0";
	values["delete_pending"] = "0";

	return insert_named_row(db, "backup_images", image_columns, values);
}

bool update_imported_file_created(ServerFilesDao& filesdao, int64 fileid, int64 created)
{
	// addFileSQL reconstructs the linkage fields consistently, but its insert path uses
	// the files.created default. Reconcile-storage overwrites that afterwards so rebuilt
	// rows keep a stable filesystem-derived timestamp instead of the import time.
	IQuery* q = filesdao.getDatabase()->Prepare("UPDATE files SET created=? WHERE id=?", false);
	q->Bind(created);
	q->Bind(fileid);
	const bool ok = q->Write();
	q->Reset();
	filesdao.getDatabase()->destroyQuery(q);
	return ok;
}

bool should_skip_dataset_entry_name(const std::string& name)
{
	return name == ".hashes" || name == ".directory_pool";
}

bool scan_dataset_file_tree_stats(const std::string& current_path, const std::string& backup_label,
	bool ignore_read_errors, bool is_root, _i64& total_size, int64& file_count,
	int64& ignored_read_errors_for_backup, SStats& stats)
{
	if (is_reconcile_stop_requested())
	{
		return false;
	}

	bool has_error = false;
	std::vector<SFile> files = getFiles(current_path, &has_error);
	if (has_error)
	{
		// With --ignore-read-errors only subtree access failures are tolerated. If the
		// backup root itself cannot be listed there is no trustworthy subset to import.
		if (!ignore_read_errors || is_root)
		{
			return false;
		}

		++ignored_read_errors_for_backup;
		++stats.ignored_read_errors;
		Server->Log("Ignoring unreadable storage subtree \"" + current_path + "\" while scanning backup \""
			+ backup_label + "\" because --ignore-read-errors was specified.", LL_WARNING);
	}

	for (size_t i = 0; i < files.size(); ++i)
	{
		if (is_reconcile_stop_requested())
		{
			return false;
		}

		if (files[i].isdir)
		{
			if (should_skip_dataset_entry_name(files[i].name))
			{
				continue;
			}
			if (!scan_dataset_file_tree_stats(join_path(current_path, files[i].name), backup_label,
				ignore_read_errors, false, total_size, file_count, ignored_read_errors_for_backup, stats))
			{
				return false;
			}
			continue;
		}

		if (files[i].issym || files[i].isspecialf)
		{
			continue;
		}

		total_size += files[i].size;
		++file_count;
	}

	return true;
}

std::string current_sqlite_timestamp(IDatabase* db)
{
	db_results res = db->Read("SELECT CURRENT_TIMESTAMP AS ts");
	if (res.empty())
	{
		return "1970-01-01 00:00:00";
	}
	return res[0]["ts"];
}

class ReconcileHashProgressCallback : public BackupServerPrepareHash::IHashProgressCallback
{
public:
	ReconcileHashProgressCallback()
		: cancelled(false)
	{
	}

	// Storage-import hashing is synchronous and can run on very large media files.
	// Polling the reconcile stop flag here keeps Ctrl-C responsive while still
	// returning through the normal import path instead of aborting mid-row.
	virtual bool should_cancel()
	{
		cancelled = cancelled || is_reconcile_stop_requested();
		return cancelled;
	}

	virtual void hash_progress(int64)
	{
	}

	bool was_cancelled() const
	{
		return cancelled;
	}

private:
	bool cancelled;
};

class ReconcileHashFileWriterNoSpaceCallback : public INotEnoughSpaceCallback
{
public:
	// Reconcile-storage does not attempt emergency cleanup while rebuilding
	// metadata files. A plain "false" here turns ENOSPC into a normal write
	// failure so the operator sees the real cause in the surrounding log message.
	virtual bool handle_not_enough_space(const std::string&)
	{
		return false;
	}
};

bool build_hash_for_file(const std::string& path, std::string& shahash, bool& cancelled)
{
	cancelled = false;

	std::auto_ptr<IFsFile> f(Server->openFile(os_file_prefix(path), MODE_READ));
	if (f.get() == NULL)
	{
		Server->Log("Could not open dataset file \"" + path + "\" for hashing. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	FsExtentIterator extent_iterator(f.get(), 512 * 1024);
	ReconcileHashProgressCallback progress_callback;
	if (BackupServer::useTreeHashing())
	{
		TreeHash treehash(NULL);
		if (!BackupServerPrepareHash::hash_sha(f.get(), &extent_iterator, true, treehash, &progress_callback))
		{
			cancelled = progress_callback.was_cancelled();
			return false;
		}
		shahash = treehash.finalize();
	}
	else
	{
		HashSha512 hashsha;
		if (!BackupServerPrepareHash::hash_sha(f.get(), &extent_iterator, true, hashsha, &progress_callback))
		{
			cancelled = progress_callback.was_cancelled();
			return false;
		}
		shahash = hashsha.finalize();
	}

	return true;
}

bool read_hash_metadata_shahash(const std::string& hashpath, int64 expected_filesize, std::string& shahash, std::string& reason)
{
	reason.clear();
	shahash.clear();

	std::auto_ptr<IFile> hashfile(Server->openFile(os_file_prefix(hashpath), MODE_READ));
	if (hashfile.get() == NULL)
	{
		reason = "metadata file cannot be opened";
		return false;
	}

	const int64 hashfilesize = read_hashdata_size(hashfile.get());
	if (hashfilesize != expected_filesize)
	{
		reason = hashfilesize == -1
			? "metadata file does not contain hashdata for a regular file"
			: "metadata file size header " + convert(hashfilesize) + " does not match file size " + convert(expected_filesize);
		return false;
	}

	FileMetadata metadata;
	if (!read_metadata(hashfile.get(), metadata))
	{
		reason = "metadata payload could not be read";
		return false;
	}

	if (metadata.shahash.size() < 16)
	{
		reason = "metadata shahash is missing or shorter than the fileindex key";
		return false;
	}

	shahash = metadata.shahash;
	return true;
}

bool write_hash_metadata_for_file(const std::string& physical_path, const std::string& hashpath, std::string& shahash, bool& cancelled)
{
	cancelled = false;

	std::auto_ptr<IFsFile> data_file(Server->openFile(os_file_prefix(physical_path), MODE_READ));
	if (data_file.get() == NULL)
	{
		Server->Log("Could not open dataset file \"" + physical_path + "\" for rebuilding metadata. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	const std::string parent_dir = ExtractFilePath(hashpath, os_file_sep());
	if (!parent_dir.empty() && !os_directory_exists(os_file_prefix(parent_dir)))
	{
		if (!os_create_dir_recursive(os_file_prefix(parent_dir)))
		{
			Server->Log("Could not create metadata directory \"" + parent_dir + "\" while rebuilding \"" + hashpath
				+ "\". " + os_last_error_str(), LL_ERROR);
			return false;
		}
	}

	if (FileExists(os_file_prefix(hashpath)))
	{
		Server->deleteFile(os_file_prefix(hashpath));
	}

	std::auto_ptr<IFile> hashoutput(Server->openFile(os_file_prefix(hashpath), MODE_RW_CREATE));
	if (hashoutput.get() == NULL)
	{
		Server->Log("Could not create metadata file \"" + hashpath + "\" while rebuilding it. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	FsExtentIterator extent_iterator(data_file.get(), 512 * 1024);
	ReconcileHashProgressCallback progress_callback;
	ReconcileHashFileWriterNoSpaceCallback no_space_callback;
	bool ok = false;
	if (BackupServer::useTreeHashing())
	{
		TreeHash treehash(NULL);
		ok = build_chunk_hashs(data_file.get(), hashoutput.get(), &no_space_callback, NULL, false, NULL, NULL, false, &treehash, &extent_iterator);
		if (ok)
		{
			shahash = treehash.finalize();
		}
	}
	else
	{
		HashSha512 hashsha;
		ok = build_chunk_hashs(data_file.get(), hashoutput.get(), &no_space_callback, NULL, false, NULL, NULL, false, &hashsha, &extent_iterator);
		if (ok)
		{
			shahash = hashsha.finalize();
		}
	}

	cancelled = progress_callback.was_cancelled();
	if (!ok || shahash.empty())
	{
		Server->Log("Could not rebuild metadata file \"" + hashpath + "\" from \"" + physical_path + "\".", LL_ERROR);
		Server->deleteFile(os_file_prefix(hashpath));
		return false;
	}

	FileMetadata metadata;
	metadata.set_shahash(shahash);
	int64 truncate_to_bytes = -1;
	if (!write_file_metadata(hashoutput.get(), &no_space_callback, metadata, false, truncate_to_bytes))
	{
		Server->Log("Could not append metadata payload to rebuilt hash file \"" + hashpath + "\".", LL_ERROR);
		hashoutput.reset();
		Server->deleteFile(os_file_prefix(hashpath));
		return false;
	}
	if (truncate_to_bytes > 0 && !os_file_truncate(os_file_prefix(hashpath), truncate_to_bytes))
	{
		Server->Log("Could not truncate rebuilt hash file \"" + hashpath + "\" to its final size. " + os_last_error_str(), LL_ERROR);
		hashoutput.reset();
		Server->deleteFile(os_file_prefix(hashpath));
		return false;
	}

	return true;
}

bool resolve_import_file_hash(const std::string& physical_path, const std::string& hashpath, int64 expected_filesize,
	bool rehash_files, SCursorProgress& progress, std::string& shahash, bool& cancelled)
{
	cancelled = false;
	shahash.clear();

	if (!rehash_files)
	{
		std::string invalid_reason;
		if (read_hash_metadata_shahash(hashpath, expected_filesize, shahash, invalid_reason))
		{
			return true;
		}

		log_with_cursor_progress(progress, "Rebuilding metadata file \"" + hashpath + "\" for \"" + physical_path
			+ "\" because the existing .hashes entry is missing or invalid (" + invalid_reason + ").", LL_WARNING);
		if (write_hash_metadata_for_file(physical_path, hashpath, shahash, cancelled))
		{
			return true;
		}

		return false;
	}

	// --rehash-files preserves the previous import behaviour. The fallback after a
	// failed metadata validation uses the same payload hashing path as the previous
	// implementation and intentionally does not touch the sidecar on disk.
	return build_hash_for_file(physical_path, shahash, cancelled);
}

bool resume_imported_file_entry(ServerFilesDao& filesdao, FileIndex& fileindex,
	const SImportedDatasetBackup& imported, const std::string& fullpath, const std::string& hashpath,
	const SFile& file_meta, SStats& stats, SCursorProgress& progress, bool& reused_entry)
{
	reused_entry = false;

	ServerFilesDao::CondInt64 entry_id = filesdao.lookupEntryIdByPath(fullpath);
	if (!entry_id.exists)
	{
		return true;
	}

	ServerFilesDao::SFindFileEntry entry = filesdao.getFileEntry(entry_id.value);
	if (!entry.exists)
	{
		return true;
	}

	// Resume skips only rows that already match the visible storage object for the
	// same backup. This keeps Ctrl-C resumptions idempotent while still allowing a
	// later run to rebuild an entry if the partially imported row is obviously for a
	// different backup or path.
	if (entry.backupid != imported.backupid
		|| entry.clientid != imported.clientid
		|| entry.filesize != file_meta.size
		|| entry.rsize != file_meta.size
		|| entry.incremental != imported.incremental
		|| entry.fullpath != fullpath
		|| entry.hashpath != hashpath)
	{
		return true;
	}

	if (file_meta.size >= link_file_min_size)
	{
		const int64 indexed_id = fileindex.get_with_cache_exact(FileIndex::SIndexKey(entry.shahash.c_str(), entry.filesize, entry.clientid));
		if (indexed_id == 0)
		{
			// A previous run may have been interrupted after the file row was written
			// but before the delayed LMDB update was persisted. Re-adding the exact key
			// here repairs the file index without rewriting the files row.
			FileIndex::put_delayed(FileIndex::SIndexKey(entry.shahash.c_str(), entry.filesize, entry.clientid), entry.id);
		}
	}

	++stats.file_rows_reused;
	reused_entry = true;
	if (stats.file_rows_reused <= 20)
	{
		log_with_cursor_progress(progress, "Reusing already imported storage file row id "
			+ convert(entry.id) + " for backup \"" + imported.clientname + "/" + imported.backup_path
			+ "\" path \"" + fullpath + "\"", LL_INFO);
	}
	return true;
}

ETreeImportResult import_dataset_file_tree_recursive(ServerFilesDao& filesdao, FileIndex& fileindex, const std::string& current_path,
	const std::string& relative_path, const std::string& visible_root,
	SImportedDatasetBackup& imported, SCursorProgress& progress, bool apply, bool ignore_read_errors, bool rehash_files,
	int64& ignored_read_errors_for_backup, SStats& stats)
{
	if (is_reconcile_stop_requested())
	{
		return ETreeImportResult_Stopped;
	}

	bool has_error = false;
	std::vector<SFile> files = getFiles(current_path, &has_error);
	if (has_error)
	{
		// Partial imports are only allowed when explicitly requested. A failed root
		// directory listing still aborts because there is otherwise no stable base.
		if (!ignore_read_errors || relative_path.empty())
		{
			return ETreeImportResult_Error;
		}

		++ignored_read_errors_for_backup;
		++stats.ignored_read_errors;
		log_with_cursor_progress(progress, "Ignoring unreadable storage subtree \"" + current_path
			+ "\" while importing backup \"" + imported.clientname + "/" + imported.backup_path
			+ "\" because --ignore-read-errors was specified.", LL_WARNING);
	}

	std::sort(files.begin(), files.end());
	for (size_t i = 0; i < files.size(); ++i)
	{
		if (is_reconcile_stop_requested())
		{
			return ETreeImportResult_Stopped;
		}

		const std::string child_relative = relative_path.empty() ? files[i].name : relative_path + os_file_sep() + files[i].name;
		const std::string physical_path = join_path(current_path, files[i].name);
		const std::string fullpath = join_path(visible_root, child_relative);
		const std::string hashpath = metadata_path_from_relative(join_path(visible_root, ".hashes"), child_relative);
		draw_cursor_progress(progress, stats.file_rows_importable + stats.file_rows_imported + 1,
			imported.clientname + "/" + imported.backup_path + " " + child_relative, false);

		if (files[i].isdir)
		{
			if (should_skip_dataset_entry_name(files[i].name))
			{
				continue;
			}
			const ETreeImportResult child_result = import_dataset_file_tree_recursive(filesdao, fileindex, physical_path, child_relative,
				visible_root, imported, progress, apply, ignore_read_errors, rehash_files, ignored_read_errors_for_backup, stats);
			if (child_result != ETreeImportResult_Ok)
			{
				return child_result;
			}
			continue;
		}

		if (files[i].issym || files[i].isspecialf)
		{
			continue;
		}

		if (imported.resume_existing_backup)
		{
			bool reused_entry = false;
			if (!resume_imported_file_entry(filesdao, fileindex, imported, fullpath, hashpath, files[i], stats, progress, reused_entry))
			{
				return ETreeImportResult_Error;
			}
			if (reused_entry)
			{
				continue;
			}
		}

		std::string shahash;
		bool hash_cancelled = false;
		if (!resolve_import_file_hash(physical_path, hashpath, files[i].size, rehash_files, progress, shahash, hash_cancelled))
		{
			if (hash_cancelled)
			{
				return ETreeImportResult_Stopped;
			}

			if (!ignore_read_errors)
			{
				return ETreeImportResult_Error;
			}

			// Read and hashing failures are skipped per file so the rest of the backup
			// can still be reconstructed from the readable content.
			++ignored_read_errors_for_backup;
			++stats.ignored_read_errors;
			log_with_cursor_progress(progress, "Ignoring unreadable file \"" + physical_path
				+ "\" while importing backup \"" + imported.clientname + "/" + imported.backup_path
				+ "\" because --ignore-read-errors was specified.", LL_WARNING);
			continue;
		}

		++stats.file_rows_importable;
		++imported.file_rows;
		if (!apply)
		{
			continue;
		}

		BackupServerHash::addFileSQL(filesdao, fileindex, static_cast<int>(imported.backupid),
			static_cast<int>(imported.clientid), imported.incremental, fullpath, hashpath, shahash,
			files[i].size, files[i].size, 0, 0, 0, true);

		const int64 fileid = filesdao.getLastId();
		const int64 created = files[i].created != 0 ? files[i].created : files[i].last_modified;
		if (created != 0 && !update_imported_file_created(filesdao, fileid, created))
		{
			return ETreeImportResult_Error;
		}

		++stats.file_rows_imported;

		// Ctrl-C is honored only after the current file row and matching fileindex
		// update are durable. That keeps the resume state per file consistent and
		// allows the next run to skip exactly the files that were already imported.
		if (is_reconcile_stop_requested())
		{
			return ETreeImportResult_Stopped;
		}
	}

	return ETreeImportResult_Ok;
}

bool update_imported_file_backup_state(IDatabase* db, int64 backupid, bool complete, const std::string& running, _i64 size_bytes)
{
	IQuery* q = db->Prepare("UPDATE backups SET complete=?, running=?, size_bytes=?, done=1 WHERE id=?", false);
	if (q == NULL)
	{
		return false;
	}
	q->Bind(complete ? 1 : 0);
	q->Bind(running);
	q->Bind(size_bytes);
	q->Bind(backupid);
	const bool ok = q->Write();
	q->Reset();
	db->destroyQuery(q);
	return ok;
}

bool insert_dataset_file_backup_row(IDatabase* db, const std::vector<STableColumnInfo>& backup_columns,
	const SImportedDatasetBackup& imported, bool apply, bool complete)
{
	if (!apply)
	{
		return true;
	}

	std::map<std::string, std::string> values;
	values["clientid"] = convert(imported.clientid);
	values["backuptime"] = imported.sqlite_time;
	values["incremental"] = convert(imported.incremental);
	values["path"] = imported.backup_path;
	values["complete"] = complete ? "1" : "0";
	values["running"] = complete ? "1970-01-01 00:00:00" : current_sqlite_timestamp(db);
	values["size_bytes"] = convert(imported.total_size);
	values["done"] = "1";

	if (!insert_named_row(db, "backups", backup_columns, values))
	{
		return false;
	}

	return true;
}

SExistingFileBackupState current_file_backup_state(IDatabase* db, const std::string& clientname, const std::string& path)
{
	SExistingFileBackupState ret;
	IQuery* q = db->Prepare("SELECT b.id, b.complete, b.done FROM backups b INNER JOIN clients c ON c.id=b.clientid WHERE c.name=? AND b.path=?", false);
	if (q == NULL)
	{
		ret.status = ELookupStatus_Error;
		return ret;
	}
	q->Bind(clientname);
	q->Bind(path);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	if (res.empty())
	{
		ret.status = ELookupStatus_NotFound;
		return ret;
	}

	ret.status = ELookupStatus_Found;
	ret.id = watoi64(res[0]["id"]);
	ret.complete = watoi(res[0]["complete"]) != 0;
	ret.done = watoi(res[0]["done"]) != 0;
	return ret;
}

bool import_storage_file_backup(IDatabase* db, const std::vector<STableColumnInfo>& backup_columns,
	const SStorageBackupEntry& entry, bool apply, bool ignore_read_errors, bool rehash_files, std::set<int64>& changed_clientids, SStats& stats)
{
	SImportedDatasetBackup imported;
	imported.clientname = entry.clientname;
	imported.backup_path = entry.backup_path;

	if (!parse_backup_time_from_path(entry.backup_path, imported.sqlite_time))
	{
		Server->Log("Cannot reconstruct file backup row for storage path \"" + entry.root_path
			+ "\" because the backup timestamp could not be parsed from path \"" + entry.backup_path + "\"", LL_WARNING);
		return true;
	}

	imported.clientid = ensure_client(db, entry.clientname, apply);
	if (imported.clientid == 0)
	{
		Server->Log("Could not ensure client row for storage-backed file backup client \"" + entry.clientname + "\"", LL_ERROR);
		return false;
	}
	if (imported.clientid > 0)
	{
		db_results older = db->Read("SELECT COUNT(*) AS c FROM backups WHERE clientid=" + convert(imported.clientid)
			+ " AND backuptime<" + sql_string(imported.sqlite_time));
		imported.incremental = (!older.empty() && watoi64(older[0]["c"]) > 0) ? 1 : 0;
	}

	const SExistingFileBackupState existing_backup = current_file_backup_state(db, entry.clientname, entry.backup_path);
	if (existing_backup.status == ELookupStatus_Error)
	{
		return false;
	}
	if (existing_backup.status == ELookupStatus_Found && existing_backup.complete)
	{
		return true;
	}
	if (existing_backup.status == ELookupStatus_Found)
	{
		imported.backupid = existing_backup.id;
		imported.resume_existing_backup = true;
	}

	++stats.dataset_backups_importable;
	int64 ignored_read_errors_for_backup = 0;

	// Dry-run is used primarily to compare the visible backup set with remove-unknown.
	// Walking every file tree here would both slow the scan down significantly and
	// suppress candidates behind restrictive permissions before they can be reported.
	// Dry-run candidates are emitted as regular log lines instead of transient cursor
	// status updates so non-interactive logs and copied terminal output still show
	// exactly which backups would be imported.
	if (!apply)
	{
		Server->Log(std::string("Would import") + " " + storage_import_action_label(entry)
			+ " file backup client \"" + entry.clientname + "\" path \"" + entry.backup_path
			+ "\" root \"" + entry.root_path + "\"", LL_INFO);
		return true;
	}

	SCursorProgress count_progress("Scanning storage file backup", 0);
	log_with_cursor_progress(count_progress, "Importing " + storage_import_action_label(entry)
		+ " file backup client \"" + entry.clientname
		+ "\" path \"" + entry.backup_path + "\" root \"" + entry.root_path + "\"", LL_INFO);

	// The storage scan computes the future backup size before the row exists so the
	// main backup table gets a correct size_bytes value immediately instead of waiting
	// for a later statistics refresh.
	if (!scan_dataset_file_tree_stats(entry.root_path, entry.clientname + "/" + entry.backup_path,
		ignore_read_errors, true, imported.total_size, imported.file_rows, ignored_read_errors_for_backup, stats))
	{
		if (is_reconcile_stop_requested())
		{
			Server->Log("Stopping storage file backup import for client \"" + entry.clientname + "\" path \""
				+ entry.backup_path + "\" before backup row creation because termination was requested.", LL_WARNING);
			return false;
		}

		log_with_cursor_progress(count_progress, "Skipping storage file backup client \"" + entry.clientname
			+ "\" path \"" + entry.backup_path
			+ "\" because its contents could not be read completely. See previous log lines for the concrete filesystem error.", LL_WARNING);
		return true;
	}
	dismiss_cursor_progress(count_progress);

	if (apply && imported.resume_existing_backup)
	{
		// A resumed import keeps the existing backup id stable so already imported
		// file rows continue to belong to the same backup. The state is switched back
		// to running/incomplete here and finalized only after the last file is done.
		if (!update_imported_file_backup_state(db, imported.backupid, false, current_sqlite_timestamp(db), imported.total_size))
		{
			return false;
		}
	}
	else if (!insert_dataset_file_backup_row(db, backup_columns, imported, apply, false))
	{
		return false;
	}

	if (apply)
	{
		if (!imported.resume_existing_backup)
		{
			imported.backupid = db->getLastInsertID();
		}
		ServerFilesDao filesdao(Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER_FILES));
		std::auto_ptr<FileIndex> fileindex(create_lmdb_files_index());
		if (fileindex.get() == NULL)
		{
			Server->Log("Could not open file index for storage-backed file backup import. The file index is missing.", LL_ERROR);
			db->Write("DELETE FROM backups WHERE id=" + convert(imported.backupid));
			return false;
		}

		bool have_resume_lookup = false;
		if (imported.resume_existing_backup)
		{
			if (!filesdao.createTemporaryPathLookupTable()
				|| !filesdao.createTemporaryPathLookupIndex())
			{
				Server->Log("Could not create temporary lookup structures for resuming storage-backed file import of backup id "
					+ convert(imported.backupid), LL_ERROR);
				return false;
			}
			filesdao.populateTemporaryPathLookupTable(static_cast<int>(imported.backupid));
			have_resume_lookup = true;
		}

		SCursorProgress import_progress("Importing storage file rows", 0);
		const int64 file_rows_imported_before = stats.file_rows_imported;
		const int64 file_rows_importable_before = stats.file_rows_importable;
		// Fileindex insertions can happen for every imported file. Routing those
		// updates into the active progress line keeps interactive imports readable
		// while preserving the same information in continuously refreshed form.
		ScopedFileIndexStatusProgress scoped_fileindex_status(&import_progress);
		// reconcile-storage may run without an available asynchronous fileindex
		// writer thread. Immediate mode keeps LMDB and the in-memory lookup cache in
		// sync for this backup import without depending on the global thread pool.
		FileIndex::begin_immediate_updates(*fileindex);
		const ETreeImportResult import_result = import_dataset_file_tree_recursive(filesdao, *fileindex, entry.root_path, std::string(),
			entry.root_path, imported, import_progress, apply, ignore_read_errors, rehash_files, ignored_read_errors_for_backup, stats);
		if (have_resume_lookup)
		{
			filesdao.dropTemporaryPathLookupIndex();
			filesdao.dropTemporaryPathLookupTable();
		}
		if (import_result != ETreeImportResult_Ok)
		{
			FileIndex::end_immediate_updates(false);
			finish_cursor_progress(import_progress, stats.file_rows_imported, import_result == ETreeImportResult_Stopped ? "stopped" : "failed");

			if (import_result == ETreeImportResult_Stopped)
			{
				if (!update_imported_file_backup_state(db, imported.backupid, false, "1970-01-01 00:00:00", imported.total_size))
				{
					return false;
				}
				Server->Log("Stopping storage file backup import for client \"" + entry.clientname + "\" path \""
					+ entry.backup_path + "\" after the current file because termination was requested. "
					"The partial import remains resumable on the next run.", LL_WARNING);
				return false;
			}

			// Filesystem and hashing errors still clean up the new backup rows on the
			// same run. Resume is reserved for operator-triggered interruption where the
			// already committed per-file state is intentionally kept.
			db->Write("DELETE FROM backups WHERE id=" + convert(imported.backupid));
			Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER_FILES)->Write("DELETE FROM files WHERE backupid=" + convert(imported.backupid));
			stats.file_rows_imported = file_rows_imported_before;
			stats.file_rows_importable = file_rows_importable_before;
			log_with_cursor_progress(import_progress, "Skipping storage file backup client \"" + entry.clientname
				+ "\" path \"" + entry.backup_path
				+ "\" because importing its file rows failed. Partial database rows were removed again. See previous log lines for the concrete filesystem or hashing error.", LL_WARNING);
			return true;
		}
		FileIndex::end_immediate_updates(true);
		if (imported.file_rows == 0)
		{
			dismiss_cursor_progress(import_progress);
		}
		else
		{
			finish_cursor_progress(import_progress, stats.file_rows_imported, "done");
		}
		if (!update_imported_file_backup_state(db, imported.backupid, true, "1970-01-01 00:00:00", imported.total_size))
		{
			return false;
		}

		// A single completion log per successfully imported backup keeps the
		// interactive output compact while still recording which backup finished and
		// how much content was reconstructed into the current database.
		Server->Log("Imported storage file backup client \"" + entry.clientname + "\" path \""
			+ entry.backup_path + "\" backupid " + convert(imported.backupid)
			+ " file rows " + convert(imported.file_rows)
			+ " size " + PrettyPrintBytes(imported.total_size), LL_INFO);

		// Per-backup reporting keeps ignored read errors attributable to the backup
		// that triggered them, instead of only showing the final global aggregate.
		if (ignored_read_errors_for_backup > 0)
		{
			Server->Log("Imported storage file backup client \"" + entry.clientname + "\" path \""
				+ entry.backup_path + "\" with " + convert(ignored_read_errors_for_backup)
				+ " ignored read error(s) because --ignore-read-errors was specified.", LL_WARNING);
		}

	}

	if (apply)
	{
		// Recomputing client rollups after every imported backup re-scans the full
		// files table each time. On large installations that made reconcile-storage
		// appear stalled immediately after the first successful import. Record the
		// touched client here and recompute its aggregates once after the visible
		// storage scan has finished.
		changed_clientids.insert(imported.clientid);
		++stats.dataset_backups_imported;
	}

	return true;
}

bool find_image_payload_in_backup_root(const std::string& backup_root, std::string& image_name, _i64& image_size)
{
	bool has_error = false;
	std::vector<SFile> files = getFiles(backup_root, &has_error);
	if (has_error)
	{
		return false;
	}

	for (size_t i = 0; i < files.size(); ++i)
	{
		if (files[i].isdir || files[i].issym || files[i].isspecialf)
		{
			continue;
		}

		const std::string ext = findextension(files[i].name);
		if (is_supported_image_extension(ext))
		{
			image_name = files[i].name;
			image_size = files[i].size;
			return true;
		}
	}

	return false;
}

std::string storage_image_letter(const SStorageBackupEntry& entry, const std::string& image_name)
{
	const std::string marker = "_Image_";
	size_t pos = entry.backup_path.find(marker);
	if (pos != std::string::npos && pos + marker.size() < entry.backup_path.size())
	{
		return entry.backup_path.substr(pos + marker.size(), 1);
	}

	pos = image_name.find("Image_");
	if (pos != std::string::npos && pos + 6 < image_name.size())
	{
		return image_name.substr(pos + 6, 1);
	}

	return std::string();
}

bool import_storage_image_backup(IDatabase* db, const std::vector<STableColumnInfo>& image_columns,
	const SStorageBackupEntry& entry, bool apply, std::set<int64>& changed_clientids, SStats& stats)
{
	std::string sqlite_time;
	if (!parse_backup_time_from_path(entry.backup_path, sqlite_time))
	{
		Server->Log("Cannot reconstruct image backup row for storage path \"" + entry.root_path
			+ "\" because the backup timestamp could not be parsed from path \"" + entry.backup_path + "\"", LL_WARNING);
		return true;
	}

	int64 clientid = ensure_client(db, entry.clientname, apply);
	if (clientid == 0)
	{
		Server->Log("Could not ensure client row for storage-backed image backup client \"" + entry.clientname + "\"", LL_ERROR);
		return false;
	}

	std::string image_name;
	_i64 image_size = 0;
	if (entry.standalone_image_file)
	{
		// Standalone image files already point at the payload, so import must not
		// search for a nested image file that does not exist in that layout.
		SFile file_meta = getFileMetadata(entry.payload_path);
		if (file_meta.isdir || file_meta.isspecialf || file_meta.name.empty())
		{
			Server->Log("Could not import storage-backed image backup client \"" + entry.clientname
				+ "\" path \"" + entry.backup_path + "\" because payload file \"" + entry.payload_path
				+ "\" is not accessible as a regular file", LL_WARNING);
			return true;
		}

		image_name = file_meta.name;
		image_size = file_meta.size;
	}
	else if (!find_image_payload_in_backup_root(entry.root_path, image_name, image_size))
	{
		Server->Log("Could not find image payload in storage path \"" + entry.root_path + "\" for client \""
			+ entry.clientname + "\" path \"" + entry.backup_path + "\"", LL_WARNING);
		return true;
	}

	int incremental = 0;
	if (clientid > 0)
	{
		db_results older = db->Read("SELECT COUNT(*) AS c FROM backup_images WHERE clientid=" + convert(clientid)
			+ " AND backuptime<" + sql_string(sqlite_time));
		incremental = (!older.empty() && watoi64(older[0]["c"]) > 0) ? 1 : 0;
	}
	const std::string image_path = entry.standalone_image_file
		? entry.payload_path
		: join_path(entry.root_path, image_name);
	const std::string image_letter = storage_image_letter(entry, image_name);

	++stats.dataset_backups_importable;

	// Image dry-runs only need to surface the visible candidate itself. Payload
	// validation remains part of apply mode so the listing stays aligned with
	// remove-unknown even when image directories are currently unreadable. As with
	// file backups, dry-run candidates are logged explicitly so they are preserved
	// in normal log files and shell transcripts.
	if (!apply)
	{
		Server->Log(std::string("Would import") + " " + storage_import_action_label(entry)
			+ " image backup client \"" + entry.clientname + "\" path \"" + entry.backup_path
			+ "\" root \"" + entry.root_path + "\"", LL_INFO);
		return true;
	}

	SCursorProgress image_progress("Importing storage image backup", 0);
	log_with_cursor_progress(image_progress,
		"Importing " + storage_import_action_label(entry) + " image backup client \""
		+ entry.clientname + "\" path \"" + entry.backup_path + "\" root \"" + entry.root_path + "\"", LL_INFO);

	if (!insert_dataset_image_backup_row(db, image_columns, clientid, sqlite_time, image_path, image_letter, incremental, image_size, apply))
	{
		return false;
	}
	if (apply)
	{
		dismiss_cursor_progress(image_progress);

		// Image imports now emit one compact completion line, matching the file
		// import behaviour and making it easier to correlate the summary with the
		// concrete storage-backed backup that just finished.
		Server->Log("Imported storage image backup client \"" + entry.clientname + "\" path \""
			+ entry.backup_path + "\" image \"" + image_name + "\" size "
			+ PrettyPrintBytes(image_size), LL_INFO);

		// Image imports participate in the same deferred rollup update as file
		// imports so one reconcile-storage run only recomputes client aggregates
		// once per touched client instead of once per imported backup.
		changed_clientids.insert(clientid);
		++stats.dataset_backups_imported;
	}

	return true;
}

bool import_missing_dataset_backups(IDatabase* db, const std::string& client_filter,
	const std::string& storage_prefix, bool apply, bool ignore_read_errors, bool rehash_files, SStats& stats)
{
	std::vector<SStorageBackupEntry> entries;
	if (!list_visible_storage_backup_entries(storage_prefix, client_filter, entries, stats))
	{
		return false;
	}

	if (entries.empty())
	{
		Server->Log("No visible storage backups found for import.", LL_INFO);
		return true;
	}

	const std::vector<STableColumnInfo> backup_columns = subset_column_info(table_column_info(db, "main", "backups"),
		std::set<std::string>({"clientid", "backuptime", "incremental", "path", "complete", "running", "size_bytes", "done"}));
	const std::vector<STableColumnInfo> image_columns = subset_column_info(table_column_info(db, "main", "backup_images"),
		std::set<std::string>({"clientid", "backuptime", "incremental", "incremental_ref", "path",
			"complete", "running", "size_bytes", "done", "version", "letter", "archived",
			"archive_timeout", "mounttime", "delete_pending"}));

	std::sort(entries.begin(), entries.end(), [](const SStorageBackupEntry& a, const SStorageBackupEntry& b)
	{
		if (a.clientname != b.clientname)
		{
			return a.clientname < b.clientname;
		}
		if (a.backup_path != b.backup_path)
		{
			return a.backup_path < b.backup_path;
		}
		return a.image < b.image;
	});

	bool ok = true;
	std::set<int64> changed_clientids;
	for (size_t i = 0; i < entries.size(); ++i)
	{
		if (is_reconcile_stop_requested())
		{
			ok = false;
			break;
		}

		const SStorageBackupEntry& entry = entries[i];
		++stats.dataset_backups_seen;

		if (entry.image)
		{
			const int exists_status = current_storage_image_backup_exists(db, entry);
			if (exists_status == ELookupStatus_Error)
			{
				ok = false;
				break;
			}
			if (exists_status == ELookupStatus_Found)
			{
				continue;
			}

			SStorageBackupEntry current_entry = entry;
			int64 existing_clientid = 0;
			const int client_lookup_status = current_client_id(db, entry.clientname, existing_clientid);
			if (client_lookup_status == ELookupStatus_Error)
			{
				ok = false;
				break;
			}
			current_entry.client_exists_in_db = client_lookup_status == ELookupStatus_Found;
			ok = ok && import_storage_image_backup(db, image_columns, current_entry, apply, changed_clientids, stats);
		}
		else
		{
			const SExistingFileBackupState existing_backup = current_file_backup_state(db, entry.clientname, entry.backup_path);
			if (existing_backup.status == ELookupStatus_Error)
			{
				ok = false;
				break;
			}
			if (existing_backup.status == ELookupStatus_Found && existing_backup.complete)
			{
				continue;
			}

			SStorageBackupEntry current_entry = entry;
			int64 existing_clientid = 0;
			const int client_lookup_status = current_client_id(db, entry.clientname, existing_clientid);
			if (client_lookup_status == ELookupStatus_Error)
			{
				ok = false;
				break;
			}
			current_entry.client_exists_in_db = client_lookup_status == ELookupStatus_Found;
			ok = ok && import_storage_file_backup(db, backup_columns, current_entry, apply, ignore_read_errors, rehash_files, changed_clientids, stats);
		}
		if (!ok)
		{
			break;
		}
	}

	if (ok && apply)
	{
		Server->Log("Updating client statistics for " + convert(changed_clientids.size())
			+ " client(s) touched by storage import. This can take a while on large databases.", LL_INFO);

		for (std::set<int64>::const_iterator it = changed_clientids.begin(); it != changed_clientids.end(); ++it)
		{
			// The deferred rollup pass preserves the same final client metadata as
			// the old per-backup updates, but reduces full-table SUM/MAX rescans to
			// one pass per touched client for the whole reconcile-storage run.
			if (!update_client_rollups(db, *it))
			{
				ok = false;
				break;
			}
		}
	}
	return ok;
}

bool recreate_missing_dataset_links(IDatabase* db, const std::string& client_filter,
	const std::string& storage_prefix, bool apply, SStats& stats)
{
	const bool file_zfs_links = BackupServer::getSnapshotMethod(false) == BackupServer::ESnapshotMethod_ZfsFile;
	const bool image_zfs_links = BackupServer::getSnapshotMethod(true) == BackupServer::ESnapshotMethod_Zfs;

	if (!file_zfs_links && !image_zfs_links)
	{
		Server->Log("Skipping dataset link recreation because neither file nor image backups use dataset-backed links.", LL_INFO);
		return true;
	}

	bool ok = true;

	if (file_zfs_links)
	{
		std::string sql = "SELECT b.id AS backupid, b.path AS backup_path, c.name AS client_name "
			"FROM backups b INNER JOIN clients c ON c.id=b.clientid";
		const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
		if (!client_filter_expr.empty())
		{
			sql += " WHERE " + client_filter_expr;
		}
		sql += " ORDER BY c.name ASC, b.path ASC";

		IQuery* q = db->Prepare(sql, false);
		IDatabaseCursor* cursor = q->Cursor();
		db_single_result row;
		int64 processed_rows = 0;
		SCursorProgress progress("Recreating file dataset links", 0);

		while (cursor->next(row))
		{
			++processed_rows;
			const std::string clientname = row_result_value(row, "client_name");
			const std::string backup_path = row_result_value(row, "backup_path");
			draw_cursor_progress(progress, processed_rows, clientname + "/" + backup_path, false);

			const std::string link_path = join_path(join_path(storage_prefix, clientname), backup_path);
			if (os_get_file_type(os_file_prefix(link_path)) != 0)
			{
				continue;
			}
			if (!SnapshotHelper::isSubvolume(false, clientname, backup_path))
			{
				continue;
			}

			const std::string mountpoint = SnapshotHelper::getMountpoint(false, clientname, backup_path);
			if (mountpoint.empty())
			{
				log_with_cursor_progress(progress, "Dataset mountpoint lookup failed for file backup id "
					+ row_result_value(row, "backupid") + " client \"" + clientname + "\" path \"" + backup_path + "\"", LL_WARNING);
				ok = false;
				continue;
			}

			++stats.dataset_links;
			log_with_cursor_progress(progress, std::string(apply ? "Recreating" : "Would recreate")
				+ " file dataset link for backup id " + row_result_value(row, "backupid")
				+ " client \"" + clientname + "\" path \"" + backup_path + "\" -> \"" + mountpoint + "\"", LL_INFO);

			if (!create_dataset_link_at_path(mountpoint, link_path, apply))
			{
				ok = false;
				continue;
			}

			if (apply)
			{
				++stats.dataset_links_created;
			}
		}

		finish_cursor_progress(progress, processed_rows, "done");
		const bool cursor_ok = !cursor->has_error();
		cursor->shutdown();
		db->destroyQuery(q);
		ok = ok && cursor_ok;
	}

	if (image_zfs_links)
	{
		std::string sql = "SELECT i.id AS backupid, i.path AS image_path, c.name AS client_name "
			"FROM backup_images i INNER JOIN clients c ON c.id=i.clientid WHERE i.complete=1";
		const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
		if (!client_filter_expr.empty())
		{
			sql += " AND " + client_filter_expr;
		}
		sql += " ORDER BY c.name ASC, i.path ASC";

		IQuery* q = db->Prepare(sql, false);
		IDatabaseCursor* cursor = q->Cursor();
		db_single_result row;
		int64 processed_rows = 0;
		SCursorProgress progress("Recreating image dataset links", 0);

		while (cursor->next(row))
		{
			++processed_rows;
			const std::string clientname = row_result_value(row, "client_name");
			const std::string image_path = row_result_value(row, "image_path");
			draw_cursor_progress(progress, processed_rows, clientname + "/" + image_path, false);

			if (!should_recreate_image_dataset_link(row))
			{
				continue;
			}

			const std::string image_folder = ExtractFilePath(image_path);
			const std::string backup_path = ExtractFileName(image_folder);
			if (image_folder.empty() || backup_path.empty())
			{
				continue;
			}

			// Image backup rows may still point at an old storage prefix after migration.
			// Recreate the visible link below the current storage prefix so the server sees
			// the same layout it would have created during the original backup run.
			const std::string link_path = join_path(join_path(storage_prefix, clientname), backup_path);
			if (os_get_file_type(os_file_prefix(link_path)) != 0)
			{
				continue;
			}
			if (!SnapshotHelper::isSubvolume(true, clientname, backup_path))
			{
				continue;
			}

			const std::string mountpoint = SnapshotHelper::getMountpoint(true, clientname, backup_path);
			if (mountpoint.empty())
			{
				log_with_cursor_progress(progress, "Dataset mountpoint lookup failed for image backup id "
					+ row_result_value(row, "backupid") + " client \"" + clientname + "\" path \"" + image_path + "\"", LL_WARNING);
				ok = false;
				continue;
			}

			++stats.dataset_links;
			log_with_cursor_progress(progress, std::string(apply ? "Recreating" : "Would recreate")
				+ " image dataset link for backup id " + row_result_value(row, "backupid")
				+ " client \"" + clientname + "\" path \"" + link_path + "\" -> \"" + mountpoint + "\"", LL_INFO);

			if (!create_dataset_link_at_path(mountpoint, link_path, apply))
			{
				ok = false;
				continue;
			}

			if (apply)
			{
				++stats.dataset_links_created;
			}
		}

		finish_cursor_progress(progress, processed_rows, "done");
		const bool cursor_ok = !cursor->has_error();
		cursor->shutdown();
		db->destroyQuery(q);
		ok = ok && cursor_ok;
	}

	return ok;
}

bool get_file_size(const std::string& path, _i64& size)
{
	if (!FileExists(os_file_prefix(path)))
	{
		size = 0;
		return true;
	}

	std::auto_ptr<IFile> f(Server->openFile(os_file_prefix(path), MODE_READ));
	if (f.get() == NULL)
	{
		Server->Log("Could not open database file \"" + path + "\" for reading. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	size = f->Size();
	if (size < 0)
	{
		size = 0;
	}
	return true;
}

void log_copy_progress(SCopyProgress& progress, const std::string& curr_file, bool force)
{
	if (progress.total_bytes <= 0)
	{
		return;
	}

	const int64 curr_time = Server->getTimeMS();
	const int64 passed_time = curr_time - progress.last_log_time;
	if (!force && passed_time < 1000)
	{
		return;
	}

	const _i64 new_bytes = progress.copied_bytes - progress.last_progress_bytes;
	const std::string speed_str = passed_time > 0
		? PrettyPrintSpeed(static_cast<size_t>((new_bytes * 1000) / passed_time))
		: PrettyPrintSpeed(0);
	const int pc_done = static_cast<int>((progress.copied_bytes * 100) / progress.total_bytes);

	Server->Log("Database backup copy progress: " + convert(pc_done) + "% ("
		+ PrettyPrintBytes(progress.copied_bytes) + " of " + PrettyPrintBytes(progress.total_bytes)
		+ ", " + speed_str + ") current file \"" + curr_file + "\"", LL_INFO);

	progress.last_progress_bytes = progress.copied_bytes;
	progress.last_log_time = curr_time;
}

void pause_cursor_progress(SCursorProgress& progress)
{
	if (progress.drawn)
	{
		// The interactive reconcile-storage workflow redraws the same terminal line
		// very frequently. ANSI line clearing avoids emitting thousands of padding
		// spaces into logs or PTY captures while still erasing wrapped leftovers.
		std::cout << "\r\033[2K";
		std::cout.flush();
		progress.drawn = false;
		if (active_cursor_progress == &progress)
		{
			active_cursor_progress = NULL;
		}
	}
}

std::string compact_progress_text(const std::string& text, size_t max_len)
{
	if (text.size() <= max_len || max_len < 16)
	{
		return text;
	}

	// Progress lines often contain long paths. Keeping both the prefix and suffix
	// preserves the operation context as well as the most relevant filename tail.
	const size_t left_len = (max_len - 3) / 2;
	const size_t right_len = max_len - 3 - left_len;
	return text.substr(0, left_len) + "..." + text.substr(text.size() - right_len);
}

void set_cursor_progress_status(SCursorProgress& progress, const std::string& msg)
{
	progress.status_message = msg;
	draw_cursor_progress(progress, progress.current, progress.detail, false);
}

void draw_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail, bool force)
{
	const int64 curr_time = Server->getTimeMS();
	const bool first_draw = !progress.drawn;
	const int64 draw_interval_ms = 500;
	if (!force && !first_draw
		&& curr_time - progress.last_draw_time < draw_interval_ms)
	{
		progress.current = current;
		progress.detail = detail;
		return;
	}

	if (first_draw || progress.last_rate_time == 0)
	{
		progress.last_rate_current = current;
		progress.last_rate_time = curr_time;
		progress.rows_per_second = 0;
	}
	else if (curr_time - progress.last_rate_time >= 1000)
	{
		const int64 passed_time = curr_time - progress.last_rate_time;
		const _i64 new_rows = current - progress.last_rate_current;
		progress.rows_per_second = passed_time > 0 ? (new_rows * 1000) / passed_time : 0;
		progress.last_rate_current = current;
		progress.last_rate_time = curr_time;
	}

	if (active_cursor_progress != NULL && active_cursor_progress != &progress)
	{
		// reconcile-storage nests multiple progress scopes. Clearing the previously
		// active line here avoids concatenating a new status or warning onto an older
		// still-visible progress line from a different scope.
		pause_cursor_progress(*active_cursor_progress);
	}

	std::string line;
	if (progress.total > 0)
	{
		const float pc_done = static_cast<float>(current) / static_cast<float>(progress.total);
		const size_t draw_segments = 30;
		size_t segments = static_cast<size_t>(pc_done * draw_segments);
		if (segments > draw_segments)
		{
			segments = draw_segments;
		}

		line = "[";
		for (size_t i = 0; i < draw_segments; ++i)
		{
			if (i < segments)
			{
				line += "=";
			}
			else if (i == segments && current < progress.total)
			{
				line += ">";
			}
			else
			{
				line += " ";
			}
		}

		int pc = static_cast<int>(pc_done * 100.f);
		if (pc > 100)
		{
			pc = 100;
		}

		line += "] " + convert(pc) + "% " + progress.label
			+ " " + convert(current) + "/" + convert(progress.total)
			+ " rows, " + convert(progress.rows_per_second) + " rows/s";
	}
	else
	{
		line = progress.label + " " + convert(current)
			+ " rows, " + convert(progress.rows_per_second) + " rows/s";
	}

	if (!progress.status_message.empty())
	{
		line += " | " + progress.status_message;
	}
	if (!detail.empty())
	{
		line += " | " + detail;
	}
	line = compact_progress_text(line, 220);

	std::cout << "\r\033[2K" << line;
	std::cout.flush();

	progress.current = current;
	progress.detail = detail;
	progress.last_draw_time = curr_time;
	progress.drawn = true;
	active_cursor_progress = &progress;
}

void finish_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail)
{
	draw_cursor_progress(progress, current, detail, true);
	if (progress.drawn)
	{
		std::cout << "\r\033[2K";
		std::cout << std::endl;
		progress.drawn = false;
		progress.status_message.clear();
		if (active_cursor_progress == &progress)
		{
			active_cursor_progress = NULL;
		}
	}
}

void dismiss_cursor_progress(SCursorProgress& progress)
{
	// Some reconcile phases use a live status line only as transient context for the
	// next regular log line. Clearing that status without emitting an extra newline
	// avoids the visually empty separator lines that would otherwise appear for
	// zero-row imports or other metadata-only operations.
	pause_cursor_progress(progress);
	progress.status_message.clear();
	progress.detail.clear();
}

void log_with_cursor_progress(SCursorProgress& progress, const std::string& msg, int loglevel)
{
	if (loglevel < LL_WARNING)
	{
		// Informational state changes are folded into the live status line so long
		// interactive runs do not scroll the terminal continuously. Warnings/errors
		// still become dedicated log lines below.
		set_cursor_progress_status(progress, msg);
		return;
	}

	if (active_cursor_progress != NULL)
	{
		pause_cursor_progress(*active_cursor_progress);
	}
	Server->Log(msg, loglevel);
	draw_cursor_progress(progress, progress.current, progress.detail, true);
}

std::string migrate_storage_path(const std::string& path,
	const std::string& new_storage_prefix,
	const std::string& clientname,
	const std::string& backup_path)
{
	if (path.empty()
		|| !is_absolute_path(path)
		|| starts_with(path, new_storage_prefix))
	{
		return path;
	}

	const std::string sep = os_file_sep();
	std::string marker = sep + clientname + sep;
	if (!backup_path.empty())
	{
		marker += backup_path;
	}

	size_t pos = path.find(marker);
	if (pos == std::string::npos)
	{
		marker = sep + clientname + sep;
		pos = path.find(marker);
	}

	if (pos == std::string::npos)
	{
		return path;
	}

	return new_storage_prefix + path.substr(pos);
}

std::vector<SColumn> table_columns(IDatabase* db, const std::string& dbname, const std::string& table)
{
	std::vector<SColumn> ret;
	db_results res = db->Read("PRAGMA " + dbname + ".table_info(" + sql_ident(table) + ")");
	for (size_t i = 0; i < res.size(); ++i)
	{
		SColumn col;
		col.name = res[i]["name"];
		col.type = res[i]["type"];
		strupper(&col.type);
		ret.push_back(col);
	}
	return ret;
}

std::vector<STableColumnInfo> table_column_info(IDatabase* db, const std::string& dbname, const std::string& table)
{
	std::vector<STableColumnInfo> ret;
	db_results res = db->Read("PRAGMA " + dbname + ".table_info(" + sql_ident(table) + ")");
	for (size_t i = 0; i < res.size(); ++i)
	{
		STableColumnInfo col;
		col.name = res[i]["name"];
		col.type = res[i]["type"];
		strupper(&col.type);
		col.default_value = res[i]["dflt_value"];
		col.notnull = watoi(res[i]["notnull"]) != 0;
		ret.push_back(col);
	}
	return ret;
}

std::vector<SColumn> intersect_columns(const std::vector<SColumn>& target,
	const std::vector<SColumn>& source)
{
	std::set<std::string> source_names;
	for (size_t i = 0; i < source.size(); ++i)
	{
		source_names.insert(source[i].name);
	}

	std::vector<SColumn> ret;
	for (size_t i = 0; i < target.size(); ++i)
	{
		if (source_names.find(target[i].name) != source_names.end())
		{
			ret.push_back(target[i]);
		}
	}
	return ret;
}

std::string column_list(const std::vector<SColumn>& columns, const std::string& prefix)
{
	std::string ret;
	for (size_t i = 0; i < columns.size(); ++i)
	{
		if (!ret.empty())
		{
			ret += ", ";
		}
		if (!prefix.empty())
		{
			ret += prefix + ".";
		}
		ret += sql_ident(columns[i].name);
	}
	return ret;
}

std::string bind_list(size_t n)
{
	std::string ret;
	for (size_t i = 0; i < n; ++i)
	{
		if (!ret.empty())
		{
			ret += ", ";
		}
		ret += "?";
	}
	return ret;
}

bool bind_row(IQuery* query, const std::vector<SColumn>& columns,
	const db_single_result& row, const std::map<std::string, std::string>& overrides)
{
	for (size_t i = 0; i < columns.size(); ++i)
	{
		std::map<std::string, std::string>::const_iterator it = overrides.find(columns[i].name);
		const std::string& value = it == overrides.end() ? row.find(columns[i].name)->second : it->second;
		if (columns[i].name == "shahash")
		{
			query->Bind(value.c_str(), static_cast<_u32>(value.size()));
		}
		else
		{
			query->Bind(value);
		}
	}
	return true;
}

bool copy_existing_file(const std::string& src, const std::string& dst, SCopyProgress& progress)
{
	if (!FileExists(os_file_prefix(src)))
	{
		return true;
	}

	std::auto_ptr<IFile> src_file(Server->openFile(os_file_prefix(src), MODE_READ_SEQUENTIAL));
	if (src_file.get() == NULL)
	{
		Server->Log("Could not open database file \"" + src + "\" for reading. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	std::auto_ptr<IFile> dst_file(Server->openFile(os_file_prefix(dst), MODE_WRITE));
	if (dst_file.get() == NULL)
	{
		Server->Log("Could not open database backup file \"" + dst + "\" for writing. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	std::vector<char> buffer(1024 * 1024);
	while (true)
	{
		bool has_error = false;
		_u32 rc = src_file->Read(&buffer[0], static_cast<_u32>(buffer.size()), &has_error);
		if (has_error)
		{
			Server->Log("Could not read database file \"" + src + "\". " + os_last_error_str(), LL_ERROR);
			return false;
		}
		if (rc == 0)
		{
			return true;
		}

		_u32 written = 0;
		while (written < rc)
		{
			bool write_error = false;
			_u32 curr = dst_file->Write(&buffer[written], rc - written, &write_error);
			if (write_error || curr == 0)
			{
				Server->Log("Could not write database backup file \"" + dst + "\". " + os_last_error_str(), LL_ERROR);
				return false;
			}
			written += curr;
			progress.copied_bytes += curr;
			log_copy_progress(progress, ExtractFileName(src), false);
		}
	}
}

bool backup_current_database(const std::string& backup_database_dir)
{
	if (backup_database_dir.empty())
	{
		Server->Log("--backup-database is mandatory", LL_ERROR);
		return false;
	}
	if (FileExists(os_file_prefix(backup_database_dir))
		|| os_directory_exists(os_file_prefix(backup_database_dir)))
	{
		Server->Log("Backup database directory \"" + backup_database_dir + "\" already exists. Refusing to continue.", LL_ERROR);
		return false;
	}
	if (!os_create_dir(os_file_prefix(backup_database_dir)))
	{
		Server->Log("Could not create database backup directory \"" + backup_database_dir + "\". " + os_last_error_str(), LL_ERROR);
		return false;
	}

	const std::string src_dir = join_path(Server->getServerWorkingDir(), "urbackup");
	const char* names[] = {
		"backup_server.db",
		"backup_server.db-wal",
		"backup_server.db-shm",
		"backup_server_files.db",
		"backup_server_files.db-wal",
		"backup_server_files.db-shm",
		"backup_server_links.db",
		"backup_server_links.db-wal",
		"backup_server_links.db-shm",
		"backup_server_link_journal.db",
		"backup_server_link_journal.db-wal",
		"backup_server_link_journal.db-shm",
		"backup_server_settings.db",
		"backup_server_settings.db-wal",
		"backup_server_settings.db-shm"
	};

	SCopyProgress progress;
	for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i)
	{
		_i64 size = 0;
		if (!get_file_size(join_path(src_dir, names[i]), size))
		{
			return false;
		}
		progress.total_bytes += size;
	}

	Server->Log("Backing up database files to \"" + backup_database_dir + "\" ("
		+ PrettyPrintBytes(progress.total_bytes) + " total)", LL_INFO);

	for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i)
	{
		if (!copy_existing_file(join_path(src_dir, names[i]), join_path(backup_database_dir, names[i]), progress))
		{
			return false;
		}
	}
	log_copy_progress(progress, "complete", true);

	if (!FileExists(os_file_prefix(join_path(backup_database_dir, "backup_server.db")))
		|| !FileExists(os_file_prefix(join_path(backup_database_dir, "backup_server_files.db")))
		|| !FileExists(os_file_prefix(join_path(backup_database_dir, "backup_server_settings.db"))))
	{
		Server->Log("Database backup is incomplete. Required database files were not copied.", LL_ERROR);
		return false;
	}

	Server->Log("Current database files were backed up to \"" + backup_database_dir + "\"", LL_INFO);
	return true;
}

bool attach_import_databases(IDatabase* db, const std::string& import_db_dir)
{
	const std::string old_main = join_path(import_db_dir, "backup_server.db");
	const std::string old_files = join_path(import_db_dir, "backup_server_files.db");
	const std::string old_settings = join_path(import_db_dir, "backup_server_settings.db");

	if (!FileExists(os_file_prefix(old_main))
		|| !FileExists(os_file_prefix(old_files))
		|| !FileExists(os_file_prefix(old_settings)))
	{
		Server->Log("Import database directory \"" + import_db_dir + "\" does not contain the required database files.", LL_ERROR);
		return false;
	}

	if (!db->Write("ATTACH DATABASE " + sql_string(old_main) + " AS old_main"))
	{
		Server->Log("Could not attach old backup_server.db", LL_ERROR);
		return false;
	}
	if (!db->Write("ATTACH DATABASE " + sql_string(old_files) + " AS old_files"))
	{
		Server->Log("Could not attach old backup_server_files.db", LL_ERROR);
		return false;
	}
	if (!db->Write("ATTACH DATABASE " + sql_string(old_settings) + " AS old_settings"))
	{
		Server->Log("Could not attach old backup_server_settings.db", LL_ERROR);
		return false;
	}
	return true;
}

bool attach_current_files(IDatabase* db)
{
	return db->Write("ATTACH DATABASE 'urbackup/backup_server_files.db' AS curr_files");
}

int current_client_id(IDatabase* db, const std::string& clientname, int64& clientid)
{
	IQuery* q = db->Prepare("SELECT id FROM clients WHERE name=?", false);
	if (q == NULL)
	{
		// A failed Prepare() means the current database handle is unusable for this
		// lookup right now. Returning a dedicated error status avoids both bogus
		// "client missing" reconstruction and NULL dereferences in the caller.
		return ELookupStatus_Error;
	}
	q->Bind(clientname);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	if (res.empty())
	{
		return ELookupStatus_NotFound;
	}
	clientid = watoi64(res[0]["id"]);
	return ELookupStatus_Found;
}

bool row_exists_id(IDatabase* db, const std::string& table, int64 id)
{
	IQuery* q = db->Prepare("SELECT id FROM " + table + " WHERE id=?", false);
	if (q == NULL)
	{
		return false;
	}
	q->Bind(id);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return !res.empty();
}

int current_backup_exists(IDatabase* db, const std::string& clientname, const std::string& path)
{
	IQuery* q = db->Prepare("SELECT b.id FROM backups b INNER JOIN clients c ON c.id=b.clientid WHERE c.name=? AND b.path=?", false);
	if (q == NULL)
	{
		// The observed crash path was: file-index setup invalidated the old DB
		// handle, the next lookup Prepare() failed with "no such table", and the
		// caller dereferenced the NULL IQuery pointer. Returning an explicit error
		// status keeps the import path from crashing and from misclassifying the
		// lookup as "backup missing".
		return ELookupStatus_Error;
	}
	q->Bind(clientname);
	q->Bind(path);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return res.empty() ? ELookupStatus_NotFound : ELookupStatus_Found;
}

bool is_malformed_file_row(const db_single_result& row, std::string& reason)
{
	if (row.find("incoming_stat_match") != row.end()
		&& row.find("incoming_stat_match")->second == "1")
	{
		/*
		SQLite .recover can sometimes classify recovered cells by the wrong
		b-tree/schema object if the source database is damaged. In the observed
		case rows from files_incoming_stat were imported as rows in files:

		  files.id        <- files_incoming_stat.id
		  files.backupid  <- files_incoming_stat.filesize
		  files.fullpath  <- files_incoming_stat.clientid       e.g. "54"
		  files.shahash   <- files_incoming_stat.backupid       e.g. integer 22689
		  files.filesize  <- files_incoming_stat.existing_clients e.g. "49,54"
		  files.created   <- files_incoming_stat.direction
		  files.rsize     <- files_incoming_stat.incremental

		Those rows are not valid file metadata. They can make backups look
		non-empty and would otherwise be reported as merely "missing files".
		Keep this as an explicit malformed-row check so removing missing
		physical files does not silently clean up recovery artifacts unless the
		operator requested --remove-malformed-file-rows.
		*/
		reason = "matches files_incoming_stat row misrecovered into files";
		return true;
	}

	if (row.find("file_backupid_type") != row.end() && row.find("file_backupid_type")->second != "integer")
	{
		reason = "backupid has type " + row.find("file_backupid_type")->second;
		return true;
	}
	if (row.find("backup_exists") != row.end() && row.find("backup_exists")->second.empty())
	{
		reason = "referenced backup is missing";
		return true;
	}
	if (row.find("file_fullpath_type") != row.end() && row.find("file_fullpath_type")->second != "text")
	{
		reason = "fullpath has type " + row.find("file_fullpath_type")->second;
		return true;
	}
	if (row.find("file_fullpath") != row.end() && !is_absolute_path(row.find("file_fullpath")->second))
	{
		reason = "fullpath is not absolute";
		return true;
	}
	if (row.find("file_hashpath_type") != row.end()
		&& row.find("file_hashpath_type")->second != "null"
		&& row.find("file_hashpath_type")->second != "text")
	{
		reason = "hashpath has type " + row.find("file_hashpath_type")->second;
		return true;
	}
	if (row.find("file_shahash_type") != row.end() && row.find("file_shahash_type")->second != "blob")
	{
		reason = "shahash has type " + row.find("file_shahash_type")->second;
		return true;
	}
	if (row.find("file_shahash_len") != row.end() && watoi64(row.find("file_shahash_len")->second) < 16)
	{
		reason = "shahash is shorter than file index key";
		return true;
	}
	if (row.find("file_filesize_type") != row.end() && row.find("file_filesize_type")->second != "integer")
	{
		reason = "filesize has type " + row.find("file_filesize_type")->second;
		return true;
	}
	if (row.find("file_filesize") != row.end() && watoi64(row.find("file_filesize")->second) < 0)
	{
		reason = "filesize is negative";
		return true;
	}
	if (row.find("file_created_type") != row.end() && row.find("file_created_type")->second != "integer")
	{
		reason = "created has type " + row.find("file_created_type")->second;
		return true;
	}
	if (row.find("file_clientid_type") != row.end() && row.find("file_clientid_type")->second != "integer")
	{
		reason = "clientid has type " + row.find("file_clientid_type")->second;
		return true;
	}
	if (row.find("file_incremental_type") != row.end() && row.find("file_incremental_type")->second != "integer")
	{
		reason = "incremental has type " + row.find("file_incremental_type")->second;
		return true;
	}
	if (row.find("file_clientid") != row.end()
		&& row.find("backup_clientid") != row.end()
		&& row.find("file_clientid")->second != row.find("backup_clientid")->second)
	{
		reason = "file clientid does not match backup clientid";
		return true;
	}

	return false;
}

std::string file_validation_select_columns();
std::string malformed_file_row_log_message(const db_single_result& row, const std::string& reason, bool apply);

bool insert_client_from_old(IDatabase* db, const db_single_result& old_backup,
	int64 old_clientid, const std::vector<SColumn>& client_columns)
{
	db_results res = db->Read("SELECT " + column_list(client_columns, "c")
		+ " FROM old_main.clients c WHERE c.id=" + convert(old_clientid));
	if (res.empty())
	{
		Server->Log("Cannot import missing client id " + convert(old_clientid) + " because it is missing in old database.", LL_ERROR);
		return false;
	}

	IQuery* q = db->Prepare("INSERT INTO clients (" + column_list(client_columns, "")
		+ ") VALUES (" + bind_list(client_columns.size()) + ")", false);
	std::map<std::string, std::string> overrides;
	bind_row(q, client_columns, res[0], overrides);
	bool ret = q->Write();
	q->Reset();
	db->destroyQuery(q);
	return ret;
}

bool insert_backup_from_old(IDatabase* db, const db_single_result& old_backup,
	const std::vector<SColumn>& backup_columns, int64 current_clientid)
{
	IQuery* q = db->Prepare("INSERT INTO backups (" + column_list(backup_columns, "")
		+ ") VALUES (" + bind_list(backup_columns.size()) + ")", false);
	std::map<std::string, std::string> overrides;
	overrides["clientid"] = convert(current_clientid);
	bind_row(q, backup_columns, old_backup, overrides);
	bool ret = q->Write();
	q->Reset();
	db->destroyQuery(q);
	return ret;
}

std::string indexable_file_row_sql_predicate(const std::string& alias)
{
	const std::string prefix = alias.empty() ? "" : alias + ".";
	return "typeof(" + prefix + "id)='integer' "
		"AND typeof(" + prefix + "backupid)='integer' "
		"AND typeof(" + prefix + "fullpath)='text' "
		"AND typeof(" + prefix + "shahash)='blob' "
		"AND length(" + prefix + "shahash)>=16 "
		"AND typeof(" + prefix + "filesize)='integer' "
		"AND " + prefix + "filesize>=0 "
		"AND typeof(" + prefix + "clientid)='integer' "
		"AND " + prefix + "clientid>=0 "
		"AND typeof(" + prefix + "incremental)='integer'";
}

bool import_file_rows(IDatabase* db_main, int64 backupid,
	const std::vector<SColumn>& file_columns, const std::string& new_storage_prefix,
	const std::string& clientname, const std::string& backup_path, bool apply, SStats& stats)
{
	db_results count_res = db_main->Read("SELECT COUNT(*) AS c FROM old_files.files WHERE backupid=" + convert(backupid));
	const int64 total_file_count = count_res.empty() ? 0 : watoi64(count_res[0]["c"]);
	db_results valid_count_res = db_main->Read("SELECT COUNT(*) AS c FROM old_files.files WHERE backupid="
		+ convert(backupid) + " AND " + indexable_file_row_sql_predicate(""));
	const int64 file_count = valid_count_res.empty() ? 0 : watoi64(valid_count_res[0]["c"]);
	const int64 skipped_file_count = total_file_count - file_count;
	stats.file_rows_importable += file_count;
	stats.file_rows_skipped_malformed += skipped_file_count;

	if (skipped_file_count > 0)
	{
		Server->Log("Skipping " + convert(skipped_file_count) + " malformed file rows while importing backup id "
			+ convert(backupid) + " client \"" + clientname + "\" path \"" + backup_path + "\"", LL_WARNING);
	}

	if (!apply)
	{
		return true;
	}

	IDatabaseCursor* cursor = db_main->Prepare("SELECT " + column_list(file_columns, "")
		+ " FROM old_files.files WHERE backupid=" + convert(backupid)
		+ " AND " + indexable_file_row_sql_predicate(""))->Cursor();

	IQuery* q_insert = db_main->Prepare("INSERT INTO curr_files.files (" + column_list(file_columns, "")
		+ ") VALUES (" + bind_list(file_columns.size()) + ")", false);
	IQuery* q_existing = db_main->Prepare("SELECT backupid, fullpath FROM curr_files.files WHERE id=?", false);

	SCursorProgress progress("Importing file rows for backup " + convert(backupid), file_count);
	_i64 processed_rows = 0;
	db_single_result row;
	while (cursor->next(row))
	{
		++processed_rows;
		draw_cursor_progress(progress, processed_rows, clientname + "/" + backup_path, false);

		std::map<std::string, std::string> overrides;
		if (row.find("fullpath") != row.end())
		{
			overrides["fullpath"] = migrate_storage_path(row["fullpath"], new_storage_prefix, clientname, backup_path);
		}
		if (row.find("hashpath") != row.end())
		{
			overrides["hashpath"] = migrate_storage_path(row["hashpath"], new_storage_prefix, clientname, backup_path);
		}

		if (row.find("id") != row.end())
		{
			q_existing->Bind(watoi64(row["id"]));
			db_results existing_rows = q_existing->Read();
			q_existing->Reset();
			if (!existing_rows.empty())
			{
				const int64 existing_backupid = watoi64(existing_rows[0]["backupid"]);
				if (existing_backupid == backupid)
				{
					++stats.file_rows_reused;
					if (stats.file_rows_reused <= 20)
					{
						log_with_cursor_progress(progress, "Reusing existing file row id " + row["id"]
							+ " for reimported backup id " + convert(backupid)
							+ " client \"" + clientname + "\" path \"" + backup_path + "\"", LL_INFO);
					}
					continue;
				}

				pause_cursor_progress(progress);
				Server->Log("Cannot import file row id " + row["id"] + " for backup id " + convert(backupid)
					+ " because the same file row id already belongs to backup id "
					+ convert(existing_backupid) + " with fullpath \"" + existing_rows[0]["fullpath"] + "\"", LL_ERROR);
				db_main->destroyQuery(q_existing);
				db_main->destroyQuery(q_insert);
				cursor->shutdown();
				return false;
			}
		}

		bind_row(q_insert, file_columns, row, overrides);
		if (!q_insert->Write())
		{
			pause_cursor_progress(progress);
			db_main->destroyQuery(q_existing);
			db_main->destroyQuery(q_insert);
			cursor->shutdown();
			return false;
		}
		q_insert->Reset();
		++stats.file_rows_imported;
	}
	finish_cursor_progress(progress, processed_rows, clientname + "/" + backup_path);

	db_main->destroyQuery(q_existing);
	db_main->destroyQuery(q_insert);
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool reconcile_missing_backups(IDatabase* db, const std::string& client_filter,
	const std::string& new_storage_prefix, bool apply, SStats& stats)
{
	std::vector<SColumn> backup_columns = intersect_columns(table_columns(db, "main", "backups"),
		table_columns(db, "old_main", "backups"));
	std::vector<SColumn> client_columns = intersect_columns(table_columns(db, "main", "clients"),
		table_columns(db, "old_main", "clients"));
	std::vector<SColumn> file_columns = intersect_columns(table_columns(db, "curr_files", "files"),
		table_columns(db, "old_files", "files"));

	if (backup_columns.empty() || client_columns.empty() || file_columns.empty())
	{
		Server->Log("Could not determine common columns for reconciliation.", LL_ERROR);
		return false;
	}

	std::string where;
	const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
	if (!client_filter_expr.empty())
	{
		where = " WHERE " + client_filter_expr;
	}

	db_results count_res = db->Read("SELECT COUNT(*) AS c FROM old_main.backups b "
		"INNER JOIN old_main.clients c ON c.id=b.clientid" + where);
	const _i64 old_backup_count = count_res.empty() ? 0 : watoi64(count_res[0]["c"]);
	SCursorProgress progress("Scanning old file backups", old_backup_count);

	IDatabaseCursor* cursor = db->Prepare("SELECT " + column_list(backup_columns, "b")
		+ ", c.name AS clientname FROM old_main.backups b INNER JOIN old_main.clients c ON c.id=b.clientid"
		+ where + " ORDER BY c.name, b.id")->Cursor();

	db_single_result row;
	while (cursor->next(row))
	{
		++stats.old_backups_seen;
		draw_cursor_progress(progress, stats.old_backups_seen, row["clientname"] + "/" + row["path"], false);

		const int64 backupid = watoi64(row["id"]);
		const int64 old_clientid = watoi64(row["clientid"]);
		const std::string clientname = row["clientname"];
		const std::string backup_path = row["path"];

		const int backup_exists_status = current_backup_exists(db, clientname, backup_path);
		if (backup_exists_status == ELookupStatus_Error)
		{
			cursor->shutdown();
			return false;
		}
		if (backup_exists_status == ELookupStatus_Found)
		{
			continue;
		}

		const std::string physical_path = join_path(join_path(new_storage_prefix, clientname), backup_path);
		if (!os_directory_exists(os_file_prefix(physical_path)))
		{
			++stats.missing_backups_skipped;
			continue;
		}

		if (row_exists_id(db, "backups", backupid))
		{
			log_with_cursor_progress(progress, "Skipping old backup id " + convert(backupid) + " for client \"" + clientname
				+ "\" because this id already exists in the current database.", LL_WARNING);
			++stats.missing_backups_skipped;
			continue;
		}

		int64 mapped_clientid = 0;
		const int client_lookup_status = current_client_id(db, clientname, mapped_clientid);
		if (client_lookup_status == ELookupStatus_Error)
		{
			cursor->shutdown();
			return false;
		}
		if (client_lookup_status != ELookupStatus_Found)
		{
			if (row_exists_id(db, "clients", old_clientid))
			{
				log_with_cursor_progress(progress, "Skipping old backup id " + convert(backupid) + " for missing client \"" + clientname
					+ "\" because old client id " + convert(old_clientid) + " already exists with another name.", LL_WARNING);
				++stats.missing_backups_skipped;
				continue;
			}
			mapped_clientid = old_clientid;
			if (apply && !insert_client_from_old(db, row, old_clientid, client_columns))
			{
				Server->Log("Could not import client \"" + clientname + "\"", LL_ERROR);
				return false;
			}
		}

		++stats.missing_backups_importable;
		if (!apply)
		{
			db_results count_res = db->Read("SELECT COUNT(*) AS c FROM old_files.files WHERE backupid="
				+ convert(backupid) + " AND " + indexable_file_row_sql_predicate(""));
			if (!count_res.empty())
			{
				stats.file_rows_importable += watoi64(count_res[0]["c"]);
			}
		}

		log_with_cursor_progress(progress, std::string(apply ? "Importing" : "Would import") + " file backup id "
			+ convert(backupid) + " client \"" + clientname + "\" path \"" + backup_path + "\"", LL_INFO);

		if (apply)
		{
			if (!insert_backup_from_old(db, row, backup_columns, mapped_clientid))
			{
				Server->Log("Could not import backup id " + convert(backupid), LL_ERROR);
				cursor->shutdown();
				return false;
			}
			if (!import_file_rows(db, backupid, file_columns, new_storage_prefix,
				clientname, backup_path, true, stats))
			{
				Server->Log("Could not import file rows for backup id " + convert(backupid), LL_ERROR);
				cursor->shutdown();
				return false;
			}
			++stats.missing_backups_imported;
		}
	}
	finish_cursor_progress(progress, stats.old_backups_seen, "done");

	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool migrate_current_file_paths(IDatabase* db, const std::string& client_filter,
	const std::string& new_storage_prefix, bool apply, bool report_malformed_rows, SStats& stats)
{
	std::string where;
	const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
	if (!client_filter_expr.empty())
	{
		where = " WHERE " + client_filter_expr;
	}

	SCursorProgress progress("Checking current file paths", 0);

	IDatabaseCursor* cursor = db->Prepare("SELECT " + file_validation_select_columns()
		+ " FROM curr_files.files f INNER JOIN backups b ON b.id=f.backupid "
		"INNER JOIN clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_update = NULL;
	if (apply)
	{
		q_update = db->Prepare("UPDATE curr_files.files SET fullpath=?, hashpath=? WHERE id=?", false);
	}

	db_single_result row;
	_i64 processed_rows = 0;
	while (cursor->next(row))
	{
		++processed_rows;
		draw_cursor_progress(progress, processed_rows, row["client_name"] + "/" + row["backup_path"], false);

		std::string malformed_reason;
		if (is_malformed_file_row(row, malformed_reason))
		{
			if (report_malformed_rows)
			{
				++stats.malformed_file_rows;
				if (stats.malformed_file_rows <= 20)
				{
					log_with_cursor_progress(progress, "Skipping malformed file row during path migration. Use --remove-malformed-file-rows to remove it. "
						+ malformed_file_row_log_message(row, malformed_reason, apply), LL_WARNING);
				}
			}
			continue;
		}

		const std::string fullpath = row["file_fullpath"];
		const std::string hashpath = row["file_hashpath"];
		const std::string migrated_fullpath = migrate_storage_path(fullpath, new_storage_prefix, row["client_name"], row["backup_path"]);
		const std::string migrated_hashpath = migrate_storage_path(hashpath, new_storage_prefix, row["client_name"], row["backup_path"]);

		if (migrated_fullpath != fullpath || migrated_hashpath != hashpath)
		{
			++stats.paths_to_migrate;
			if (stats.paths_to_migrate <= 20)
			{
				log_with_cursor_progress(progress, std::string(apply ? "Migrating" : "Would migrate") + " file path id "
					+ row["file_id"] + " from \"" + fullpath + "\" to \"" + migrated_fullpath + "\"", LL_INFO);
			}
			if (apply)
			{
				q_update->Bind(migrated_fullpath);
				q_update->Bind(migrated_hashpath);
				q_update->Bind(watoi64(row["file_id"]));
				if (!q_update->Write())
				{
					pause_cursor_progress(progress);
					db->destroyQuery(q_update);
					cursor->shutdown();
					return false;
				}
				q_update->Reset();
				++stats.paths_migrated;
			}
		}
	}
	finish_cursor_progress(progress, processed_rows, "done");

	if (q_update != NULL)
	{
		db->destroyQuery(q_update);
	}
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

std::string file_validation_select_columns()
{
	return "f.id AS file_id, f.backupid AS file_backupid, f.fullpath AS file_fullpath, "
		"f.hashpath AS file_hashpath, f.shahash AS file_shahash, f.filesize AS file_filesize, "
		"f.created AS file_created, f.rsize AS file_rsize, "
		"f.clientid AS file_clientid, f.incremental AS file_incremental, "
		"typeof(f.backupid) AS file_backupid_type, typeof(f.fullpath) AS file_fullpath_type, "
		"typeof(f.hashpath) AS file_hashpath_type, typeof(f.shahash) AS file_shahash_type, "
		"typeof(f.filesize) AS file_filesize_type, typeof(f.created) AS file_created_type, "
		"typeof(f.rsize) AS file_rsize_type, typeof(f.clientid) AS file_clientid_type, "
		"typeof(f.incremental) AS file_incremental_type, length(f.shahash) AS file_shahash_len, "
		"quote(f.backupid) AS file_backupid_quoted, quote(f.fullpath) AS file_fullpath_quoted, "
		"quote(f.hashpath) AS file_hashpath_quoted, quote(f.shahash) AS file_shahash_quoted, "
		"quote(f.filesize) AS file_filesize_quoted, quote(f.created) AS file_created_quoted, "
		"quote(f.rsize) AS file_rsize_quoted, quote(f.clientid) AS file_clientid_quoted, "
		"quote(f.incremental) AS file_incremental_quoted, "
		"b.id AS backup_exists, c.id AS backup_clientid, c.name AS client_name, b.path AS backup_path, "
		"EXISTS(SELECT 1 FROM curr_files.files_incoming_stat s WHERE s.id=f.id "
		"AND f.backupid=s.filesize AND f.fullpath=CAST(s.clientid AS TEXT) "
		"AND f.shahash=s.backupid AND f.filesize=s.existing_clients "
		"AND f.created=s.direction AND f.rsize=s.incremental "
		"AND f.clientid IS NULL AND f.incremental IS NULL) AS incoming_stat_match";
}

std::string malformed_file_row_log_message(const db_single_result& row, const std::string& reason, bool apply)
{
	return std::string(apply ? "Removing" : "Would remove") + " malformed file row id "
		+ row_result_value(row, "file_id") + " reason \"" + reason + "\""
		+ " backupid " + row_result_value(row, "file_backupid_quoted") + " type " + row_result_value(row, "file_backupid_type")
		+ " backup_exists " + row_result_value(row, "backup_exists", "<missing>")
		+ " client \"" + row_result_value(row, "client_name", "<missing-client>") + "\""
		+ " file clientid " + row_result_value(row, "file_clientid_quoted") + " type " + row_result_value(row, "file_clientid_type")
		+ " backup clientid " + row_result_value(row, "backup_clientid", "<missing>")
		+ " backup path \"" + row_result_value(row, "backup_path", "<missing-backup>") + "\""
		+ " incremental " + row_result_value(row, "file_incremental_quoted") + " type " + row_result_value(row, "file_incremental_type")
		+ " filesize " + row_result_value(row, "file_filesize_quoted") + " type " + row_result_value(row, "file_filesize_type")
		+ " created " + row_result_value(row, "file_created_quoted") + " type " + row_result_value(row, "file_created_type")
		+ " rsize " + row_result_value(row, "file_rsize_quoted") + " type " + row_result_value(row, "file_rsize_type")
		+ " fullpath " + row_result_value(row, "file_fullpath_quoted") + " type " + row_result_value(row, "file_fullpath_type")
		+ " hashpath " + row_result_value(row, "file_hashpath_quoted") + " type " + row_result_value(row, "file_hashpath_type")
		+ " shahash type " + row_result_value(row, "file_shahash_type")
		+ " len " + row_result_value(row, "file_shahash_len")
		+ " incoming_stat_match " + row_result_value(row, "incoming_stat_match");
}

bool remove_malformed_file_rows(IDatabase* db, const std::string& client_filter, bool apply, SStats& stats)
{
	std::string where;
	const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
	if (!client_filter_expr.empty())
	{
		where = " WHERE " + client_filter_expr;
	}

	SCursorProgress progress("Checking malformed file rows", 0);
	IDatabaseCursor* cursor = db->Prepare("SELECT " + file_validation_select_columns()
		+ " FROM curr_files.files f LEFT JOIN backups b ON b.id=f.backupid "
		+ "LEFT JOIN clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_delete = NULL;
	if (apply)
	{
		q_delete = db->Prepare("DELETE FROM curr_files.files WHERE id=?", false);
	}

	db_single_result row;
	_i64 processed_rows = 0;
	while (cursor->next(row))
	{
		++processed_rows;
		draw_cursor_progress(progress, processed_rows,
			row_result_value(row, "client_name", "<missing-client>") + "/" + row_result_value(row, "backup_path", "<missing-backup>"), false);

		std::string reason;
		if (is_malformed_file_row(row, reason))
		{
			++stats.malformed_file_rows;
			if (stats.malformed_file_rows <= 20)
			{
				log_with_cursor_progress(progress, malformed_file_row_log_message(row, reason, apply), LL_WARNING);
			}
			if (apply)
			{
				q_delete->Bind(watoi64(row["file_id"]));
				if (!q_delete->Write())
				{
					pause_cursor_progress(progress);
					db->destroyQuery(q_delete);
					cursor->shutdown();
					return false;
				}
				q_delete->Reset();
				++stats.malformed_file_rows_removed;
			}
		}
	}
	finish_cursor_progress(progress, processed_rows, "done");

	if (q_delete != NULL)
	{
		db->destroyQuery(q_delete);
	}
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool remove_missing_file_rows(IDatabase* db, const std::string& client_filter,
	const std::string& new_storage_prefix, bool apply, bool report_malformed_rows, SStats& stats)
{
	std::string where;
	const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
	if (!client_filter_expr.empty())
	{
		where = " WHERE " + client_filter_expr;
	}

	SCursorProgress progress("Checking missing file rows", 0);

	IDatabaseCursor* cursor = db->Prepare("SELECT " + file_validation_select_columns()
		+ " FROM curr_files.files f INNER JOIN backups b ON b.id=f.backupid "
		+ "INNER JOIN clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_delete = NULL;
	if (apply)
	{
		q_delete = db->Prepare("DELETE FROM curr_files.files WHERE id=?", false);
	}

	db_single_result row;
	_i64 processed_rows = 0;
	while (cursor->next(row))
	{
		++processed_rows;
		const std::string detail = row["client_name"] + "/" + row["backup_path"] + " checking filesystem";
		draw_cursor_progress(progress, processed_rows, detail, false);

		std::string malformed_reason;
		if (is_malformed_file_row(row, malformed_reason))
		{
			if (report_malformed_rows)
			{
				++stats.malformed_file_rows;
				if (stats.malformed_file_rows <= 20)
				{
					log_with_cursor_progress(progress, "Skipping malformed file row during missing-file scan. Use --remove-malformed-file-rows to remove it. "
						+ malformed_file_row_log_message(row, malformed_reason, apply), LL_WARNING);
				}
			}
			continue;
		}

		const std::string physical_path = migrate_storage_path(row["file_fullpath"], new_storage_prefix, row["client_name"], row["backup_path"]);
		if (!path_exists(physical_path))
		{
			++stats.missing_files;
			if (stats.missing_files <= 20)
			{
				log_with_cursor_progress(progress, std::string(apply ? "Removing" : "Would remove") + " missing file row id "
					+ row["file_id"] + " backupid " + row["file_backupid"]
					+ " client \"" + row["client_name"] + "\""
					+ " clientid " + row["file_clientid_quoted"] + " type " + row["file_clientid_type"]
					+ " backup path \"" + row["backup_path"] + "\""
					+ " incremental " + row["file_incremental_quoted"] + " type " + row["file_incremental_type"]
					+ " filesize " + row["file_filesize_quoted"] + " type " + row["file_filesize_type"]
					+ " stored fullpath " + row["file_fullpath_quoted"] + " type " + row["file_fullpath_type"]
					+ " expected physical path \"" + physical_path + "\""
					+ " hashpath " + row["file_hashpath_quoted"] + " type " + row["file_hashpath_type"], LL_INFO);
			}
			if (apply)
			{
				q_delete->Bind(watoi64(row["file_id"]));
				if (!q_delete->Write())
				{
					pause_cursor_progress(progress);
					db->destroyQuery(q_delete);
					cursor->shutdown();
					return false;
				}
				q_delete->Reset();
				++stats.missing_files_removed;
			}
		}
	}
	finish_cursor_progress(progress, processed_rows, "done");

	if (q_delete != NULL)
	{
		db->destroyQuery(q_delete);
	}
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

std::string valid_file_row_sql_predicate()
{
	return "typeof(f.backupid)='integer' "
		"AND typeof(f.fullpath)='text' "
		"AND substr(f.fullpath,1,1)='/' "
		"AND typeof(f.shahash)='blob' "
		"AND length(f.shahash)>=16 "
		"AND typeof(f.filesize)='integer' "
		"AND f.filesize>=0 "
		"AND typeof(f.clientid)='integer' "
		"AND f.clientid=b.clientid "
		"AND typeof(f.incremental)='integer' "
		"AND (typeof(f.hashpath)='null' OR typeof(f.hashpath)='text') "
		"AND NOT EXISTS(SELECT 1 FROM curr_files.files_incoming_stat s WHERE s.id=f.id "
		"AND f.backupid=s.filesize AND f.fullpath=CAST(s.clientid AS TEXT) "
		"AND f.shahash=s.backupid AND f.filesize=s.existing_clients "
		"AND f.created=s.direction AND f.rsize=s.incremental "
		"AND f.clientid IS NULL AND f.incremental IS NULL)";
}

bool drop_empty_file_backups(IDatabase* db, const std::string& client_filter, bool apply, SStats& stats)
{
	std::string where = " WHERE NOT EXISTS (SELECT 1 FROM curr_files.files f WHERE f.backupid=b.id AND "
		+ valid_file_row_sql_predicate() + ")";
	const std::string client_filter_expr = client_filter_sql_expr(client_filter, "c.name");
	if (!client_filter_expr.empty())
	{
		where += " AND " + client_filter_expr;
	}

	// The COUNT/NOT EXISTS pre-scan can run for a noticeable amount of time on large
	// installations before the first cursor row is available. Logging this upfront
	// makes the otherwise quiet phase explicit to the operator.
	Server->Log("Checking for empty file backups. Counting candidates first; this can take a while on large databases.", LL_INFO);

	db_results count_res = db->Read("SELECT COUNT(*) AS c "
		"FROM backups b INNER JOIN clients c ON c.id=b.clientid" + where);
	const _i64 row_count = count_res.empty() ? 0 : watoi64(count_res[0]["c"]);
	SCursorProgress progress("Checking empty file backups", row_count);

	IDatabaseCursor* cursor = db->Prepare("SELECT b.id AS id, c.name AS clientname, b.path AS path "
		"FROM backups b INNER JOIN clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_delete = NULL;
	if (apply)
	{
		q_delete = db->Prepare("DELETE FROM backups WHERE id=?", false);
	}

	db_single_result row;
	_i64 processed_rows = 0;
	while (cursor->next(row))
	{
		++processed_rows;
		draw_cursor_progress(progress, processed_rows, row["clientname"] + "/" + row["path"], false);

		++stats.empty_backups;
		log_with_cursor_progress(progress, std::string(apply ? "Removing" : "Would remove") + " empty file backup id "
			+ row["id"] + " client \"" + row["clientname"] + "\" path \"" + row["path"] + "\"", LL_INFO);
		if (apply)
		{
			q_delete->Bind(watoi64(row["id"]));
			if (!q_delete->Write())
			{
				pause_cursor_progress(progress);
				db->destroyQuery(q_delete);
				cursor->shutdown();
				return false;
			}
			q_delete->Reset();
			++stats.empty_backups_removed;
		}
	}
	finish_cursor_progress(progress, processed_rows, "done");

	if (q_delete != NULL)
	{
		db->destroyQuery(q_delete);
	}
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

void log_summary(const SStats& stats, bool apply)
{
	Server->Log("Reconcile-storage summary:", LL_INFO);
	Server->Log("  old file backups seen: " + convert(stats.old_backups_seen), LL_INFO);
	Server->Log("  missing file backups importable: " + convert(stats.missing_backups_importable), LL_INFO);
	Server->Log("  missing file backups skipped: " + convert(stats.missing_backups_skipped), LL_INFO);
	Server->Log(std::string("  missing file backups ") + (apply ? "imported: " : "would import: ")
		+ convert(apply ? stats.missing_backups_imported : stats.missing_backups_importable), LL_INFO);
	Server->Log(std::string("  file rows ") + (apply ? "imported: " : "would import: ")
		+ convert(apply ? stats.file_rows_imported : stats.file_rows_importable), LL_INFO);
	if (apply || stats.file_rows_reused > 0)
	{
		Server->Log("  file rows reused: " + convert(stats.file_rows_reused), LL_INFO);
	}
	Server->Log("  malformed file rows skipped during import: " + convert(stats.file_rows_skipped_malformed), LL_INFO);
	Server->Log(std::string("  dataset links ") + (apply ? "created: " : "would create: ")
		+ convert(apply ? stats.dataset_links_created : stats.dataset_links), LL_INFO);
	Server->Log("  visible storage backups seen: " + convert(stats.dataset_backups_seen), LL_INFO);
	Server->Log("  visible storage backups importable: " + convert(stats.dataset_backups_importable), LL_INFO);
	Server->Log(std::string("  visible storage backups ") + (apply ? "imported: " : "would import: ")
		+ convert(apply ? stats.dataset_backups_imported : stats.dataset_backups_importable), LL_INFO);
	Server->Log("  ignored read errors during storage import: " + convert(stats.ignored_read_errors), LL_INFO);
	Server->Log("  visible storage links with unreachable targets: " + convert(stats.broken_storage_link_targets), LL_INFO);
	Server->Log(std::string("  file paths ") + (apply ? "migrated: " : "would migrate: ")
		+ convert(apply ? stats.paths_migrated : stats.paths_to_migrate), LL_INFO);
	Server->Log(std::string("  missing file rows ") + (apply ? "removed: " : "would remove: ")
		+ convert(apply ? stats.missing_files_removed : stats.missing_files), LL_INFO);
	Server->Log(std::string("  malformed file rows ") + (apply ? "removed: " : "would remove: ")
		+ convert(apply ? stats.malformed_file_rows_removed : stats.malformed_file_rows), LL_INFO);
	Server->Log(std::string("  empty file backups ") + (apply ? "removed: " : "would remove: ")
		+ convert(apply ? stats.empty_backups_removed : stats.empty_backups), LL_INFO);
}
}

int reconcile_storage()
{
	// reconcile-storage is often run interactively for long migrations. Installing
	// local SIGINT/SIGTERM handlers lets Ctrl-C stop between consistent file-level
	// units instead of aborting immediately in the middle of the import code paths.
	ScopedReconcileSignalHandlers reconcile_signal_handlers;

	std::string import_db_dir = strip_trailing_sep(Server->getServerParameter("import_db"));
	if (import_db_dir.empty())
	{
		import_db_dir = strip_trailing_sep(Server->getServerParameter("import_db_dir"));
	}
	if (import_db_dir.empty())
	{
		import_db_dir = strip_trailing_sep(Server->getServerParameter("old_db_dir"));
	}
	const std::string backup_database_dir = strip_trailing_sep(Server->getServerParameter("backup_database"));
	const std::string client_filter = Server->getServerParameter("client");
	const bool apply = Server->getServerParameter("apply") == "true";
	const bool skip_db_backup = Server->getServerParameter("skip_db_backup") == "true";
	const bool migrate_existing_file_paths = Server->getServerParameter("migrate_existing_file_paths") == "true";
	const bool remove_missing_files = Server->getServerParameter("remove_missing_files") == "true";
	const bool remove_malformed_file_rows_enabled = Server->getServerParameter("remove_malformed_file_rows") == "true";
	const bool recreate_missing_dataset_links_enabled = Server->getServerParameter("recreate_missing_dataset_links") == "true";
	bool import_missing_dataset_backups_enabled = Server->getServerParameter("import_storage") == "true";
	const bool ignore_read_errors = Server->getServerParameter("ignore_read_errors") == "true";
	const bool rehash_files = Server->getServerParameter("rehash_files") == "true";
	const bool regenerate_index = Server->getServerParameter("regenerate_index") == "true";
	if (!import_missing_dataset_backups_enabled)
	{
		import_missing_dataset_backups_enabled = Server->getServerParameter("import_missing_dataset_backups") == "true";
	}
	const bool drop_empty_backups = Server->getServerParameter("drop_empty_backups") == "true";

	if (skip_db_backup)
	{
		Server->Log("Skipping pre-run database backup because --skip-db-backup was specified.", LL_WARNING);
	}
	else if (!backup_current_database(backup_database_dir))
	{
		return 1;
	}

	Server->Log(std::string("Starting reconcile-storage in ") + (apply ? "apply" : "dry-run") + " mode", LL_INFO);
	if (!client_filter.empty())
	{
		Server->Log("Restricting reconciliation to client set \"" + client_filter + "\"", LL_INFO);
	}
	if (import_db_dir.empty())
	{
		Server->Log("No --import-db specified. Missing backup import is disabled; only current database actions will run.", LL_INFO);
	}

	open_server_database(true);
	open_settings_database();

	IDatabase* db = Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER);
	if (db == NULL)
	{
		Server->Log("Could not open current UrBackup database", LL_ERROR);
		return 1;
	}

	// Dataset-link recreation relies on the same global snapshot method state as the
	// normal backup paths. Reconcile-storage must initialize that state explicitly
	// before deciding whether file/image backups use ZFS-backed datasets.
	BackupServer::testSnapshotAvailability(db);

	ServerSettings settings(db);
	std::string new_storage_prefix = strip_trailing_sep(Server->getServerParameter("new_storage_prefix"));
	if (new_storage_prefix.empty())
	{
		new_storage_prefix = strip_trailing_sep(settings.getSettings()->backupfolder);
	}
	if (new_storage_prefix.empty())
	{
		Server->Log("Could not determine new storage prefix. Please specify --new-storage-prefix.", LL_ERROR);
		return 1;
	}
	Server->Log("Using new storage prefix \"" + new_storage_prefix + "\"", LL_INFO);

	const bool have_import_db = !import_db_dir.empty();
	const bool storage_import_needs_file_index = apply && import_missing_dataset_backups_enabled;
	const bool storage_import_uses_detached_files_connection = apply && import_missing_dataset_backups_enabled;
	const bool current_files_rows_will_change = apply
		&& (remove_malformed_file_rows_enabled || remove_missing_files || have_import_db || import_missing_dataset_backups_enabled);

	// Creating the file index can rebuild backup_server_files.db and internally
	// destroys all open SQLite handles. Reconcile-storage therefore has to do this
	// before attaching curr_files or old_* databases and must reacquire the main
	// server handle afterwards, otherwise later lookups can hit stale connections.
	if (storage_import_needs_file_index && !ensure_file_index_available_for_reconcile(false))
	{
		Server->Log("Could not create or open the file index required for storage-backed file imports.", LL_ERROR);
		return 1;
	}

	db = Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER);
	if (db == NULL)
	{
		Server->Log("Could not reopen current UrBackup database after preparing the file index", LL_ERROR);
		return 1;
	}

	if (!attach_current_files(db))
	{
		Server->Log("Could not attach current files database.", LL_ERROR);
		return 1;
	}
	if (have_import_db && !attach_import_databases(db, import_db_dir))
	{
		return 1;
	}

	SStats stats;
	bool ok = true;

	if (apply && !storage_import_uses_detached_files_connection)
	{
		/*
		reconcile-storage attaches backup_server_files.db as curr_files to the
		main backup_server.db connection. A single BEGIN IMMEDIATE on this
		connection is enough to make writes to both databases transactional.
		Do not also open a write transaction on URBACKUPDB_SERVER_FILES here:
		that is a second SQLite connection to the same physical
		backup_server_files.db file and it can block on the write lock already
		held by this connection. The externally visible symptom is an endless
		SQLITE_BUSY loop on "BEGIN IMMEDIATE" even when reconcile-storage is the
		only running urbackupsrv process.
		*/
		if (!db->BeginWriteTransaction())
		{
			Server->Log("Could not start reconcile-storage write transaction.", LL_ERROR);
			return 1;
		}
	}
	else if (apply)
	{
		/*
		The storage import path reconstructs file rows via ServerFilesDao on
		URBACKUPDB_SERVER_FILES, i.e. through a second SQLite connection to
		backup_server_files.db. Holding a long-lived BEGIN IMMEDIATE on the main
		connection at the same time would self-deadlock those file-row inserts with
		SQLITE_BUSY. This path therefore runs without the outer transaction and uses
		the per-backup cleanup already present in import_storage_file_backup() to
		remove partial rows on failure.
		*/
		Server->Log("Running apply mode without outer reconcile-storage transaction because --import-storage writes file rows via a detached SQLite connection.", LL_INFO);
	}

	if (remove_malformed_file_rows_enabled)
	{
		ok = ok && remove_malformed_file_rows(db, client_filter, apply, stats);
	}
	else
	{
		Server->Log("Skipping malformed file row removal because --remove-malformed-file-rows was not specified.", LL_INFO);
	}

	if (have_import_db)
	{
		ok = ok && reconcile_missing_backups(db, client_filter, new_storage_prefix, apply, stats);
	}
	else
	{
		Server->Log("Skipping missing backup import because --import-db was not specified.", LL_INFO);
	}

	if (recreate_missing_dataset_links_enabled)
	{
		warn_if_dataset_detection_failed();
		ok = ok && recreate_missing_dataset_links(db, client_filter, new_storage_prefix, apply, stats);
	}
	else
	{
		Server->Log("Skipping dataset link recreation because --recreate-missing-dataset-links was not specified.", LL_INFO);
	}

	if (import_missing_dataset_backups_enabled)
	{
		ok = ok && import_missing_dataset_backups(db, client_filter, new_storage_prefix, apply, ignore_read_errors, rehash_files, stats);
	}
	else
	{
		Server->Log("Skipping visible storage backup import because --import-storage was not specified.", LL_INFO);
	}

	bool report_malformed_rows_in_scan = !remove_malformed_file_rows_enabled;
	if (migrate_existing_file_paths)
	{
		ok = ok && migrate_current_file_paths(db, client_filter, new_storage_prefix, apply,
			report_malformed_rows_in_scan, stats);
		report_malformed_rows_in_scan = false;
	}
	else
	{
		Server->Log("Skipping existing file path migration because --migrate-existing-file-paths was not specified.", LL_INFO);
	}
	if (remove_missing_files)
	{
		ok = ok && remove_missing_file_rows(db, client_filter, new_storage_prefix, apply,
			report_malformed_rows_in_scan, stats);
		report_malformed_rows_in_scan = false;
	}
	else
	{
		Server->Log("Skipping missing file row removal because --remove-missing-files was not specified.", LL_INFO);
	}
	if (drop_empty_backups)
	{
		ok = ok && drop_empty_file_backups(db, client_filter, apply, stats);
	}
	else
	{
		Server->Log("Skipping empty backup removal because --drop-empty-backups was not specified.", LL_INFO);
	}

	if (apply && !storage_import_uses_detached_files_connection)
	{
		if (ok)
		{
			db->EndTransaction();
		}
		else
		{
			db->RollbackTransaction();
		}
	}

	log_summary(stats, apply);
	if (stats.broken_storage_link_targets > 0)
	{
		// Broken backup links usually indicate that dataset-backed storage is not
		// mounted at the moment. Running remove-unknown in that state would interpret
		// those backups as missing and can therefore delete references to valid data.
		Server->Log("Detected " + convert(stats.broken_storage_link_targets)
			+ " visible backup link(s) with unreachable targets. Do not run remove-unknown until the corresponding datasets or mount targets are available again, otherwise this can cause data loss.", LL_WARNING);
	}

	if (!apply)
	{
		Server->Log("Dry-run complete. No database changes were applied.", LL_INFO);
	}
	else if (ok)
	{
		// Only operations that insert or delete rows in curr_files.files invalidate the
		// LMDB index. Path-only updates and backup-table-only changes leave it valid.
		if (current_files_rows_will_change)
		{
			delete_file_index();
			if (regenerate_index)
			{
				if (!ensure_file_index_available_for_reconcile(true))
				{
					Server->Log("Apply completed, but regenerating the file index failed.", LL_ERROR);
					return 1;
				}
				Server->Log("Apply complete. File index was regenerated as requested.", LL_INFO);
			}
			else
			{
				Server->Log("Apply complete. File index was removed and will be rebuilt on next startup/check.", LL_INFO);
			}
		}
		else
		{
			Server->Log("Apply complete. File index remained unchanged because no indexed file rows were modified.", LL_INFO);
		}
	}

	return ok ? 0 : 1;
}
