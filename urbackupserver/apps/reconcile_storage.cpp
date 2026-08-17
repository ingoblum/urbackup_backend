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
#include "../server_settings.h"
#include "../../Interface/DatabaseCursor.h"
#include "../../Interface/File.h"
#include "../../urbackupcommon/os_functions.h"
#include "../../stringtools.h"

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

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
		paths_to_migrate(0), paths_migrated(0), missing_files(0), missing_files_removed(0),
		empty_backups(0), empty_backups_removed(0)
	{
	}

	int64 old_backups_seen;
	int64 missing_backups_importable;
	int64 missing_backups_imported;
	int64 missing_backups_skipped;
	int64 file_rows_importable;
	int64 file_rows_imported;
	int64 paths_to_migrate;
	int64 paths_migrated;
	int64 missing_files;
	int64 missing_files_removed;
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

bool attach_old_databases(IDatabase* db, const std::string& old_db_dir)
{
	const std::string old_main = join_path(old_db_dir, "backup_server.db");
	const std::string old_files = join_path(old_db_dir, "backup_server_files.db");
	const std::string old_settings = join_path(old_db_dir, "backup_server_settings.db");

	if (!FileExists(os_file_prefix(old_main))
		|| !FileExists(os_file_prefix(old_files))
		|| !FileExists(os_file_prefix(old_settings)))
	{
		Server->Log("Old database directory \"" + old_db_dir + "\" does not contain the required database files.", LL_ERROR);
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

bool import_file_rows(IDatabase* db_files, IDatabase* db_main, int64 backupid,
	const std::vector<SColumn>& file_columns, const std::string& new_storage_prefix,
	const std::string& clientname, const std::string& backup_path, bool apply, SStats& stats)
{
	db_results count_res = db_main->Read("SELECT COUNT(*) AS c FROM old_files.files WHERE backupid=" + convert(backupid));
	const int64 file_count = count_res.empty() ? 0 : watoi64(count_res[0]["c"]);
	stats.file_rows_importable += file_count;

	if (!apply)
	{
		return true;
	}

	IDatabaseCursor* cursor = db_main->Prepare("SELECT " + column_list(file_columns, "")
		+ " FROM old_files.files WHERE backupid=" + convert(backupid))->Cursor();

	IQuery* q_insert = db_files->Prepare("INSERT INTO files (" + column_list(file_columns, "")
		+ ") VALUES (" + bind_list(file_columns.size()) + ")", false);

	db_single_result row;
	while (cursor->next(row))
	{
		std::map<std::string, std::string> overrides;
		if (row.find("fullpath") != row.end())
		{
			overrides["fullpath"] = migrate_storage_path(row["fullpath"], new_storage_prefix, clientname, backup_path);
		}
		if (row.find("hashpath") != row.end())
		{
			overrides["hashpath"] = migrate_storage_path(row["hashpath"], new_storage_prefix, clientname, backup_path);
		}

		bind_row(q_insert, file_columns, row, overrides);
		if (!q_insert->Write())
		{
			db_files->destroyQuery(q_insert);
			cursor->shutdown();
			return false;
		}
		q_insert->Reset();
		++stats.file_rows_imported;
	}

	db_files->destroyQuery(q_insert);
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool reconcile_missing_backups(IDatabase* db, IDatabase* db_files, const std::string& client_filter,
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

	IDatabaseCursor* cursor = db->Prepare("SELECT " + column_list(backup_columns, "b")
		+ ", c.name AS clientname FROM old_main.backups b INNER JOIN old_main.clients c ON c.id=b.clientid"
		+ where + " ORDER BY c.name, b.id")->Cursor();

	db_single_result row;
	while (cursor->next(row))
	{
		++stats.old_backups_seen;

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
			Server->Log("Skipping old backup id " + convert(backupid) + " for client \"" + clientname
				+ "\" because this id already exists in the current database.", LL_WARNING);
			++stats.missing_backups_skipped;
			continue;
		}

		int64 mapped_clientid = 0;
		if (!current_client_id(db, clientname, mapped_clientid))
		{
			if (row_exists_id(db, "clients", old_clientid))
			{
				Server->Log("Skipping old backup id " + convert(backupid) + " for missing client \"" + clientname
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
			db_results count_res = db->Read("SELECT COUNT(*) AS c FROM old_files.files WHERE backupid=" + convert(backupid));
			if (!count_res.empty())
			{
				stats.file_rows_importable += watoi64(count_res[0]["c"]);
			}
		}

		Server->Log(std::string(apply ? "Importing" : "Would import") + " file backup id "
			+ convert(backupid) + " client \"" + clientname + "\" path \"" + backup_path + "\"", LL_INFO);

		if (apply)
		{
			if (!insert_backup_from_old(db, row, backup_columns, mapped_clientid))
			{
				Server->Log("Could not import backup id " + convert(backupid), LL_ERROR);
				cursor->shutdown();
				return false;
			}
			if (!import_file_rows(db_files, db, backupid, file_columns, new_storage_prefix,
				clientname, backup_path, true, stats))
			{
				Server->Log("Could not import file rows for backup id " + convert(backupid), LL_ERROR);
				cursor->shutdown();
				return false;
			}
			++stats.missing_backups_imported;
		}
	}

	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool migrate_current_file_paths(IDatabase* db, IDatabase* db_files, const std::string& client_filter,
	const std::string& new_storage_prefix, bool apply, SStats& stats)
{
	if (!db_files->Write("ATTACH DATABASE 'urbackup/backup_server.db' AS main_db"))
	{
		Server->Log("Could not attach current backup_server.db to files database.", LL_ERROR);
		return false;
	}

	std::string where;
	if (!client_filter.empty())
	{
		where = " WHERE c.name=" + sql_string(client_filter);
	}

	IDatabaseCursor* cursor = db_files->Prepare(
		"SELECT f.id AS id, f.fullpath AS fullpath, f.hashpath AS hashpath, c.name AS clientname, b.path AS backuppath "
		"FROM files f INNER JOIN main_db.backups b ON b.id=f.backupid "
		"INNER JOIN main_db.clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_update = NULL;
	if (apply)
	{
		q_update = db_files->Prepare("UPDATE files SET fullpath=?, hashpath=? WHERE id=?", false);
	}

	db_single_result row;
	while (cursor->next(row))
	{
		const std::string fullpath = row["fullpath"];
		const std::string hashpath = row["hashpath"];
		const std::string migrated_fullpath = migrate_storage_path(fullpath, new_storage_prefix, row["clientname"], row["backuppath"]);
		const std::string migrated_hashpath = migrate_storage_path(hashpath, new_storage_prefix, row["clientname"], row["backuppath"]);

		if (migrated_fullpath != fullpath || migrated_hashpath != hashpath)
		{
			++stats.paths_to_migrate;
			if (stats.paths_to_migrate <= 20)
			{
				Server->Log(std::string(apply ? "Migrating" : "Would migrate") + " file path id "
					+ row["id"] + " from \"" + fullpath + "\" to \"" + migrated_fullpath + "\"", LL_INFO);
			}
			if (apply)
			{
				q_update->Bind(migrated_fullpath);
				q_update->Bind(migrated_hashpath);
				q_update->Bind(watoi64(row["id"]));
				if (!q_update->Write())
				{
					db_files->destroyQuery(q_update);
					cursor->shutdown();
					return false;
				}
				q_update->Reset();
				++stats.paths_migrated;
			}
		}
	}

	if (q_update != NULL)
	{
		db_files->destroyQuery(q_update);
	}
	db_files->Write("DETACH DATABASE main_db");
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool remove_missing_file_rows(IDatabase* db_files, const std::string& client_filter,
	const std::string& new_storage_prefix, bool apply, SStats& stats)
{
	if (!db_files->Write("ATTACH DATABASE 'urbackup/backup_server.db' AS main_db"))
	{
		Server->Log("Could not attach current backup_server.db to files database.", LL_ERROR);
		return false;
	}

	std::string where;
	if (!client_filter.empty())
	{
		where = " WHERE c.name=" + sql_string(client_filter);
	}

	IDatabaseCursor* cursor = db_files->Prepare(
		"SELECT f.id AS id, f.fullpath AS fullpath, c.name AS clientname, b.path AS backuppath "
		"FROM files f INNER JOIN main_db.backups b ON b.id=f.backupid "
		"INNER JOIN main_db.clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_delete = NULL;
	if (apply)
	{
		q_delete = db_files->Prepare("DELETE FROM files WHERE id=?", false);
	}

	db_single_result row;
	while (cursor->next(row))
	{
		const std::string physical_path = migrate_storage_path(row["fullpath"], new_storage_prefix, row["clientname"], row["backuppath"]);
		if (!path_exists(physical_path))
		{
			++stats.missing_files;
			if (stats.missing_files <= 20)
			{
				Server->Log(std::string(apply ? "Removing" : "Would remove") + " missing file row id "
					+ row["id"] + " path \"" + physical_path + "\"", LL_INFO);
			}
			if (apply)
			{
				q_delete->Bind(watoi64(row["id"]));
				if (!q_delete->Write())
				{
					db_files->destroyQuery(q_delete);
					cursor->shutdown();
					return false;
				}
				q_delete->Reset();
				++stats.missing_files_removed;
			}
		}
	}

	if (q_delete != NULL)
	{
		db_files->destroyQuery(q_delete);
	}
	db_files->Write("DETACH DATABASE main_db");
	const bool ret = !cursor->has_error();
	cursor->shutdown();
	return ret;
}

bool drop_empty_file_backups(IDatabase* db, const std::string& client_filter, bool apply, SStats& stats)
{
	std::string where = " WHERE NOT EXISTS (SELECT 1 FROM curr_files.files f WHERE f.backupid=b.id)";
	if (!client_filter.empty())
	{
		where += " AND c.name=" + sql_string(client_filter);
	}

	IDatabaseCursor* cursor = db->Prepare("SELECT b.id AS id, c.name AS clientname, b.path AS path "
		"FROM backups b INNER JOIN clients c ON c.id=b.clientid" + where)->Cursor();

	IQuery* q_delete = NULL;
	if (apply)
	{
		q_delete = db->Prepare("DELETE FROM backups WHERE id=?", false);
	}

	db_single_result row;
	while (cursor->next(row))
	{
		++stats.empty_backups;
		Server->Log(std::string(apply ? "Removing" : "Would remove") + " empty file backup id "
			+ row["id"] + " client \"" + row["clientname"] + "\" path \"" + row["path"] + "\"", LL_INFO);
		if (apply)
		{
			q_delete->Bind(watoi64(row["id"]));
			if (!q_delete->Write())
			{
				db->destroyQuery(q_delete);
				cursor->shutdown();
				return false;
			}
			q_delete->Reset();
			++stats.empty_backups_removed;
		}
	}

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
	Server->Log(std::string("  file paths ") + (apply ? "migrated: " : "would migrate: ")
		+ convert(apply ? stats.paths_migrated : stats.paths_to_migrate), LL_INFO);
	Server->Log(std::string("  missing file rows ") + (apply ? "removed: " : "would remove: ")
		+ convert(apply ? stats.missing_files_removed : stats.missing_files), LL_INFO);
	Server->Log(std::string("  empty file backups ") + (apply ? "removed: " : "would remove: ")
		+ convert(apply ? stats.empty_backups_removed : stats.empty_backups), LL_INFO);
}
}

int reconcile_storage()
{
	const std::string old_db_dir = strip_trailing_sep(Server->getServerParameter("old_db_dir"));
	const std::string backup_database_dir = strip_trailing_sep(Server->getServerParameter("backup_database"));
	const std::string client_filter = Server->getServerParameter("client");
	const bool apply = Server->getServerParameter("apply") == "true";
	const bool skip_db_backup = Server->getServerParameter("skip_db_backup") == "true";
	const bool migrate_existing_file_paths = Server->getServerParameter("migrate_existing_file_paths") == "true";
	const bool remove_missing_files = Server->getServerParameter("remove_missing_files") == "true";
	const bool drop_empty_backups = Server->getServerParameter("drop_empty_backups") == "true";

	if (old_db_dir.empty())
	{
		Server->Log("--old-db-dir is mandatory", LL_ERROR);
		return 1;
	}

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

	open_server_database(true);
	open_settings_database();

	IDatabase* db = Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER);
	IDatabase* db_files = Server->getDatabase(Server->getThreadID(), URBACKUPDB_SERVER_FILES);
	if (db == NULL || db_files == NULL)
	{
		Server->Log("Could not open current UrBackup databases", LL_ERROR);
		return 1;
	}

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
	if (!attach_old_databases(db, old_db_dir))
	{
		return 1;
	}

	SStats stats;
	bool ok = true;

	if (apply)
	{
		db->BeginWriteTransaction();
		db_files->BeginWriteTransaction();
	}

	ok = ok && reconcile_missing_backups(db, db_files, client_filter, new_storage_prefix, apply, stats);
	if (migrate_existing_file_paths)
	{
		ok = ok && migrate_current_file_paths(db, db_files, client_filter, new_storage_prefix, apply, stats);
	}
	else
	{
		Server->Log("Skipping existing file path migration because --migrate-existing-file-paths was not specified.", LL_INFO);
	}
	if (remove_missing_files)
	{
		ok = ok && remove_missing_file_rows(db_files, client_filter, new_storage_prefix, apply, stats);
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
			db_files->EndTransaction();
			db->EndTransaction();
		}
		else
		{
			db_files->RollbackTransaction();
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
