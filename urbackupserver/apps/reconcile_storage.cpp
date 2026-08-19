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
#include "../../Interface/DatabaseCursor.h"
#include "../../Interface/File.h"
#include "../../urbackupcommon/os_functions.h"
#include "../../urbackupcommon/TreeHash.h"
#include "../../stringtools.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <set>
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
		dataset_backups_importable(0), dataset_backups_imported(0),
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
	std::string root_path;
	std::string clientname;
	std::string backup_path;
};

struct SImportedDatasetBackup
{
	SImportedDatasetBackup()
		: clientid(0), backupid(0), incremental(0), total_size(0), file_rows(0)
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
};

// The dataset-link reconciliation path reuses the interactive cursor progress helpers
// below, so forward declarations keep the helper order stable and avoid reshuffling
// already debugged output code.
void draw_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail, bool force);
void finish_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail);
void log_with_cursor_progress(SCursorProgress& progress, const std::string& msg, int loglevel);
bool current_client_id(IDatabase* db, const std::string& clientname, int64& clientid);
bool current_backup_exists(IDatabase* db, const std::string& clientname, const std::string& path);
std::vector<STableColumnInfo> table_column_info(IDatabase* db, const std::string& dbname, const std::string& table);
std::string column_list(const std::vector<SColumn>& columns, const std::string& prefix);
std::string bind_list(size_t n);
bool should_skip_dataset_entry_name(const std::string& name);

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

bool path_exists(const std::string& path)
{
	if (path.empty())
	{
		return false;
	}
	const std::string prefixed = os_file_prefix(path);
	return FileExists(prefixed) || os_directory_exists(prefixed);
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

bool current_image_backup_exists(IDatabase* db, const std::string& clientname, const std::string& backup_path)
{
	IQuery* q = db->Prepare("SELECT i.id FROM backup_images i INNER JOIN clients c ON c.id=i.clientid WHERE c.name=? AND i.path LIKE ?", false);
	q->Bind(clientname);
	q->Bind("%/" + clientname + "/" + backup_path + "/%");
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return !res.empty();
}

int64 ensure_client(IDatabase* db, const std::string& clientname, bool apply)
{
	int64 clientid = 0;
	if (current_client_id(db, clientname, clientid))
	{
		return clientid;
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
	return db->Write("UPDATE clients SET "
		"bytes_used_files=COALESCE((SELECT SUM(rsize) FROM curr_files.files WHERE clientid=" + clientid_str + "),0), "
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

bool list_visible_storage_backup_entries(const std::string& storage_prefix, const std::string& client_filter,
	std::vector<SStorageBackupEntry>& entries)
{
	bool has_error = false;
	std::vector<SFile> clients = getFiles(storage_prefix, &has_error);
	if (has_error)
	{
		return false;
	}

	std::sort(clients.begin(), clients.end());
	for (size_t i = 0; i < clients.size(); ++i)
	{
		if ((!clients[i].isdir && !clients[i].issym) || clients[i].isspecialf)
		{
			continue;
		}
		if (!client_filter.empty() && clients[i].name != client_filter)
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
			if ((!backups[j].isdir && !backups[j].issym) || backups[j].isspecialf)
			{
				continue;
			}
			if (should_skip_dataset_entry_name(backups[j].name))
			{
				continue;
			}

			SStorageBackupEntry entry;
			entry.image = is_image_backup_name(backups[j].name);
			entry.symlink = backups[j].issym;
			entry.root_path = join_path(client_root, backups[j].name);
			entry.clientname = clients[i].name;
			entry.backup_path = backups[j].name;
			entries.push_back(entry);
		}
	}

	return true;
}

bool insert_dataset_file_backup_row(IDatabase* db, const std::vector<STableColumnInfo>& backup_columns,
	const SImportedDatasetBackup& imported, bool apply)
{
	if (!apply)
	{
		return true;
	}

	// Reconstructed backup rows are filled with the same semantically relevant fields
	// the server uses during normal operation. Optional later-added columns are only
	// populated when their names are known here so schema extensions keep their defaults.
	std::map<std::string, std::string> values;
	values["clientid"] = convert(imported.clientid);
	values["backuptime"] = imported.sqlite_time;
	values["incremental"] = convert(imported.incremental);
	values["path"] = imported.backup_path;
	values["complete"] = "1";
	values["running"] = "1970-01-01 00:00:00";
	values["size_bytes"] = convert(imported.total_size);
	values["done"] = "1";

	if (!insert_named_row(db, "backups", backup_columns, values))
	{
		return false;
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

bool scan_dataset_file_tree_stats(const std::string& current_path, _i64& total_size, int64& file_count)
{
	bool has_error = false;
	std::vector<SFile> files = getFiles(current_path, &has_error);
	if (has_error)
	{
		return false;
	}

	for (size_t i = 0; i < files.size(); ++i)
	{
		if (files[i].isdir)
		{
			if (should_skip_dataset_entry_name(files[i].name))
			{
				continue;
			}
			if (!scan_dataset_file_tree_stats(join_path(current_path, files[i].name), total_size, file_count))
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

bool build_hash_for_file(const std::string& path, std::string& shahash)
{
	std::auto_ptr<IFsFile> f(Server->openFile(os_file_prefix(path), MODE_READ));
	if (f.get() == NULL)
	{
		Server->Log("Could not open dataset file \"" + path + "\" for hashing. " + os_last_error_str(), LL_ERROR);
		return false;
	}

	FsExtentIterator extent_iterator(f.get(), 512 * 1024);
	if (BackupServer::useTreeHashing())
	{
		TreeHash treehash(NULL);
		if (!BackupServerPrepareHash::hash_sha(f.get(), &extent_iterator, true, treehash))
		{
			return false;
		}
		shahash = treehash.finalize();
	}
	else
	{
		HashSha512 hashsha;
		if (!BackupServerPrepareHash::hash_sha(f.get(), &extent_iterator, true, hashsha))
		{
			return false;
		}
		shahash = hashsha.finalize();
	}

	return true;
}

bool import_dataset_file_tree_recursive(ServerFilesDao& filesdao, FileIndex& fileindex, const std::string& current_path,
	const std::string& relative_path, const std::string& visible_root,
	SImportedDatasetBackup& imported, SCursorProgress& progress, bool apply, SStats& stats)
{
	bool has_error = false;
	std::vector<SFile> files = getFiles(current_path, &has_error);
	if (has_error)
	{
		return false;
	}

	std::sort(files.begin(), files.end());
	for (size_t i = 0; i < files.size(); ++i)
	{
		const std::string child_relative = relative_path.empty() ? files[i].name : relative_path + os_file_sep() + files[i].name;
		const std::string physical_path = join_path(current_path, files[i].name);
		draw_cursor_progress(progress, stats.file_rows_importable + stats.file_rows_imported + 1,
			imported.clientname + "/" + imported.backup_path + " " + child_relative, false);

		if (files[i].isdir)
		{
			if (should_skip_dataset_entry_name(files[i].name))
			{
				continue;
			}
			if (!import_dataset_file_tree_recursive(filesdao, fileindex, physical_path, child_relative,
				visible_root, imported, progress, apply, stats))
			{
				return false;
			}
			continue;
		}

		if (files[i].issym || files[i].isspecialf)
		{
			continue;
		}

		std::string shahash;
		if (!build_hash_for_file(physical_path, shahash))
		{
			return false;
		}

		const std::string fullpath = join_path(visible_root, child_relative);
		std::string hashpath = join_path(join_path(visible_root, ".hashes"), child_relative);
		if (!FileExists(os_file_prefix(hashpath)))
		{
			hashpath.clear();
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
			return false;
		}

		++stats.file_rows_imported;
	}

	return true;
}

bool import_storage_file_backup(IDatabase* db, const std::vector<STableColumnInfo>& backup_columns,
	const SStorageBackupEntry& entry, bool apply, SStats& stats)
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

	// The storage scan computes the future backup size before the row exists so the
	// main backup table gets a correct size_bytes value immediately instead of waiting
	// for a later statistics refresh.
	SCursorProgress count_progress("Scanning storage file backup", 0);
	if (!scan_dataset_file_tree_stats(entry.root_path, imported.total_size, imported.file_rows))
	{
		return false;
	}
	finish_cursor_progress(count_progress, 0, "done");

	++stats.dataset_backups_importable;
	log_with_cursor_progress(count_progress, std::string(apply ? "Importing" : "Would import")
		+ " storage-backed file backup client \"" + entry.clientname + "\" path \"" + entry.backup_path
		+ "\" root \"" + entry.root_path + "\"", LL_INFO);

	if (!insert_dataset_file_backup_row(db, backup_columns, imported, apply))
	{
		return false;
	}

	if (apply)
	{
		imported.backupid = db->getLastInsertID();
	}

	ServerFilesDao filesdao(Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER_FILES));
	std::auto_ptr<FileIndex> fileindex(create_lmdb_files_index());
	SCursorProgress import_progress("Importing storage file rows", 0);
	if (!import_dataset_file_tree_recursive(filesdao, *fileindex, entry.root_path, std::string(),
		entry.root_path, imported, import_progress, apply, stats))
	{
		finish_cursor_progress(import_progress, stats.file_rows_imported, "failed");
		if (apply)
		{
			db->Write("DELETE FROM backups WHERE id=" + convert(imported.backupid));
			Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER_FILES)->Write("DELETE FROM files WHERE backupid=" + convert(imported.backupid));
		}
		return false;
	}
	finish_cursor_progress(import_progress, stats.file_rows_imported, "done");
	FileIndex::flush();

	if (apply && !update_client_rollups(db, imported.clientid))
	{
		return false;
	}

	if (apply)
	{
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
		if (ext == "raw" || ext == "vhd" || ext == "vhdz" || ext == "vhdx" || ext == "vhdxz")
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
	const SStorageBackupEntry& entry, bool apply, SStats& stats)
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
	if (!find_image_payload_in_backup_root(entry.root_path, image_name, image_size))
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
	const std::string image_path = join_path(entry.root_path, image_name);
	const std::string image_letter = storage_image_letter(entry, image_name);

	++stats.dataset_backups_importable;
	SCursorProgress image_progress("Importing storage image backup", 0);
	log_with_cursor_progress(image_progress,
		std::string(apply ? "Importing" : "Would import") + " storage-backed image backup client \""
		+ entry.clientname + "\" path \"" + entry.backup_path + "\" root \"" + entry.root_path + "\"", LL_INFO);

	if (!insert_dataset_image_backup_row(db, image_columns, clientid, sqlite_time, image_path, image_letter, incremental, image_size, apply))
	{
		return false;
	}
	if (apply)
	{
		++stats.dataset_backups_imported;
	}

	if (apply && !update_client_rollups(db, clientid))
	{
		return false;
	}

	return true;
}

bool import_missing_dataset_backups(IDatabase* db, const std::string& client_filter,
	const std::string& storage_prefix, bool apply, SStats& stats)
{
	std::vector<SStorageBackupEntry> entries;
	if (!list_visible_storage_backup_entries(storage_prefix, client_filter, entries))
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
	SCursorProgress progress("Scanning visible storage backups", static_cast<_i64>(entries.size()));
	for (size_t i = 0; i < entries.size(); ++i)
	{
		const SStorageBackupEntry& entry = entries[i];
		++stats.dataset_backups_seen;
		draw_cursor_progress(progress, static_cast<_i64>(i + 1), entry.clientname + "/" + entry.backup_path, false);

		const bool exists = entry.image
			? current_image_backup_exists(db, entry.clientname, entry.backup_path)
			: current_backup_exists(db, entry.clientname, entry.backup_path);
		if (exists)
		{
			continue;
		}

		if (entry.image)
		{
			ok = ok && import_storage_image_backup(db, image_columns, entry, apply, stats);
		}
		else
		{
			ok = ok && import_storage_file_backup(db, backup_columns, entry, apply, stats);
		}
		if (!ok)
		{
			break;
		}
	}

	finish_cursor_progress(progress, static_cast<_i64>(entries.size()), "done");
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
		if (!client_filter.empty())
		{
			sql += " WHERE c.name=" + sql_string(client_filter);
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
		if (!client_filter.empty())
		{
			sql += " AND c.name=" + sql_string(client_filter);
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
		std::cout << "\r";
		for (size_t i = 0; i < progress.max_line_length; ++i)
		{
			std::cout << " ";
		}
		std::cout << "\r";
		std::cout.flush();
	}
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

		line = "\r[";
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
		line = "\r" + progress.label + " " + convert(current)
			+ " rows, " + convert(progress.rows_per_second) + " rows/s";
	}

	if (!detail.empty())
	{
		line += " " + detail;
	}

	if (line.size() > progress.max_line_length)
	{
		progress.max_line_length = line.size();
	}
	while (line.size() < progress.max_line_length)
	{
		line += " ";
	}

	std::cout << line;
	std::cout.flush();

	progress.current = current;
	progress.detail = detail;
	progress.last_draw_time = curr_time;
	progress.drawn = true;
}

void finish_cursor_progress(SCursorProgress& progress, _i64 current, const std::string& detail)
{
	draw_cursor_progress(progress, current, detail, true);
	if (progress.drawn)
	{
		std::cout << std::endl;
		progress.drawn = false;
		progress.max_line_length = 0;
	}
}

void log_with_cursor_progress(SCursorProgress& progress, const std::string& msg, int loglevel)
{
	pause_cursor_progress(progress);
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

bool current_client_id(IDatabase* db, const std::string& clientname, int64& clientid)
{
	IQuery* q = db->Prepare("SELECT id FROM clients WHERE name=?", false);
	q->Bind(clientname);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	if (res.empty())
	{
		return false;
	}
	clientid = watoi64(res[0]["id"]);
	return true;
}

bool row_exists_id(IDatabase* db, const std::string& table, int64 id)
{
	IQuery* q = db->Prepare("SELECT id FROM " + table + " WHERE id=?", false);
	q->Bind(id);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return !res.empty();
}

bool current_backup_exists(IDatabase* db, const std::string& clientname, const std::string& path)
{
	IQuery* q = db->Prepare("SELECT b.id FROM backups b INNER JOIN clients c ON c.id=b.clientid WHERE c.name=? AND b.path=?", false);
	q->Bind(clientname);
	q->Bind(path);
	db_results res = q->Read();
	q->Reset();
	db->destroyQuery(q);
	return !res.empty();
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
	if (!client_filter.empty())
	{
		where = " WHERE c.name=" + sql_string(client_filter);
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

		if (current_backup_exists(db, clientname, backup_path))
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
		if (!current_client_id(db, clientname, mapped_clientid))
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
	if (!client_filter.empty())
	{
		where = " WHERE c.name=" + sql_string(client_filter);
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
	if (!client_filter.empty())
	{
		where = " WHERE c.name=" + sql_string(client_filter);
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
	if (!client_filter.empty())
	{
		where = " WHERE c.name=" + sql_string(client_filter);
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
	if (!client_filter.empty())
	{
		where += " AND c.name=" + sql_string(client_filter);
	}

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
	std::string import_db_dir = strip_trailing_sep(Server->getServerParameter("import_db_dir"));
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
	const bool import_missing_dataset_backups_enabled = Server->getServerParameter("import_missing_dataset_backups") == "true";
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
		Server->Log("Restricting reconciliation to client \"" + client_filter + "\"", LL_INFO);
	}
	if (import_db_dir.empty())
	{
		Server->Log("No --import-db-dir specified. Missing backup import is disabled; only current database actions will run.", LL_INFO);
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

	if (!attach_current_files(db))
	{
		Server->Log("Could not attach current files database.", LL_ERROR);
		return 1;
	}
	const bool have_import_db = !import_db_dir.empty();
	if (have_import_db && !attach_import_databases(db, import_db_dir))
	{
		return 1;
	}

	SStats stats;
	bool ok = true;

	if (apply)
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
		Server->Log("Skipping missing backup import because --import-db-dir was not specified.", LL_INFO);
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
		ok = ok && import_missing_dataset_backups(db, client_filter, new_storage_prefix, apply, stats);
	}
	else
	{
		Server->Log("Skipping visible storage backup import because --import-missing-dataset-backups was not specified.", LL_INFO);
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

	if (apply)
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

	if (!apply)
	{
		Server->Log("Dry-run complete. No database changes were applied.", LL_INFO);
	}
	else if (ok)
	{
		delete_file_index();
		Server->Log("Apply complete. File index was removed and will be rebuilt on next startup/check.", LL_INFO);
	}

	return ok ? 0 : 1;
}
