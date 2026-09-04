#include <string>
#include <string>
#include <cstring>
#include <iostream>
#include <vector>
#include "../stringtools.h"
#include "../urbackupcommon/os_functions.h"
#include <stdlib.h>
#ifndef _WIN32
#include <unistd.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pwd.h>
extern char **environ;
#endif

#define DEF_Server
#include "../Server.h"

const int mode_btrfs=0;
const int mode_zfs=1;
const int mode_zfs_file=2;

IServer *Server;

#ifdef _WIN32
#include <Windows.h>

bool CopyFolder(std::wstring src, std::wstring dst)
{
	if(!os_create_dir(dst))
		return false;

	std::vector<SFile> curr_files=getFiles(src);
	for(size_t i=0;i<curr_files.size();++i)
	{
		if(curr_files[i].isdir)
		{
			bool b=CopyFolder(src+os_file_sep()+curr_files[i].name, dst+os_file_sep()+curr_files[i].name);
			if(!b)
				return false;
		}
		else
		{
			if(!os_create_hardlink(dst+os_file_sep()+curr_files[i].name, src+os_file_sep()+curr_files[i].name, false, NULL) )
			{
				BOOL b=CopyFileW( (src+os_file_sep()+curr_files[i].name).c_str(), (dst+os_file_sep()+curr_files[i].name).c_str(), FALSE);
				if(!b)
				{
					return false;
				}
			}
		}
	}

	return true;
}
#endif


std::string getBackupfolderPath(int mode)
{
	std::string fn_name;
	if(mode==mode_btrfs)
	{
		fn_name = "backupfolder";
	}
	else if(mode==mode_zfs)
	{
		fn_name = "dataset";
	}
	else if(mode==mode_zfs_file)
	{
		fn_name = "dataset_file";
	}
	else
	{
		return std::string();
	}

	std::string fn;
#ifdef _WIN32
	fn=trim(getFile(fn_name));
#else
	fn=trim(getFile("/etc/urbackup/"+fn_name));
#endif
	if(fn.find("\n")!=std::string::npos)
		fn=getuntil("\n", fn);
	if(fn.find("\r")!=std::string::npos)
		fn=getuntil("\r", fn);
	
	return fn;
}

std::string sanitize_zfs_name(std::string name)
{
	std::string res;
	res.reserve(name.size());
	for (size_t i = 0; i < name.size(); ++i)
	{
		char ch = name[i];
		if (ch == '[')
		{
			res += ':';
		}
		else if (ch == ']')
		{
			// remove/ignore
		}
		else if ((ch >= 'a' && ch <= 'z') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= '0' && ch <= '9') ||
			ch == '_' || ch == '.' || ch == '-' || ch == ':' || ch == '/')
		{
			res += ch;
		}
		else
		{
			res += '_';
		}
	}
	return res;
}

// Frühere Fehlerursache bei Profil-Clients (z.B. ingo-server:media):
// conv_filename() ersetzte Doppelpunkte (':') unkonditioniert durch Unterstriche ('_').
// Dadurch wurde der ZFS-Dataset-Pfad für Profilclients von "ingo-server:media" fälschlicherweise
// zu "ingo-server_media" umgeformt (z.B. data/urbackup/files/ingo-server_media/260711-0105).
// Da ZFS-Datasets für Profile jedoch regulär Doppelpunkte im Namen tragen, schlug jede
// Operation (zfs list, zfs destroy etc.) mit "dataset does not exist" fehl.
// Für mode_zfs und mode_zfs_file wird daher sanitize_zfs_name() verwendet, welches Doppelpunkte
// erhält und native Klammer-Profile wie [media] in :media überführt.
std::string handleFilename(std::string fn, int mode = 0)
{
	if(mode == mode_zfs || mode == mode_zfs_file)
	{
		fn = sanitize_zfs_name(fn);
	}
	else
	{
		fn = conv_filename(fn);
	}
	if(fn == ".." || fn.find("..") != std::string::npos)
	{
		return "";
	}
	return fn;
}

std::string get_subvolume_folder(int mode, const std::string& backupfolder, const std::string& clientname, const std::string& name)
{
	std::string res = backupfolder + os_file_sep() + clientname + os_file_sep() + name;
	if (mode == mode_zfs)
	{
		res = sanitize_zfs_name(res);
	}
	return res;
}

#ifndef _WIN32
int exec_wait(const std::string& path, bool keep_stdout, ...)
{
	va_list vl;
	va_start(vl, keep_stdout);
	
	std::vector<char*> args;
	args.push_back(const_cast<char*>(path.c_str()));
	
	while(true)
	{
		const char* p = va_arg(vl, const char*);
		if(p==NULL) break;
		args.push_back(const_cast<char*>(p));
	}
	va_end(vl);
	
	args.push_back(NULL);
	
	pid_t child_pid = fork();
	
	if(child_pid==0)
	{
		environ = new char*[1];
		*environ=NULL;
		
		if(!keep_stdout)
		{
			int nullfd = open("/dev/null", O_WRONLY);
			
			if(nullfd!=-1)
			{
				if(dup2(nullfd, 1)==-1)
				{
					return -1;
				}
				
				if(dup2(nullfd, 2)==-1)
				{
					return -1;
				}
			}
			else
			{
				return -1;
			}
		}
		
		int rc = execvp(path.c_str(), args.data());
		exit(rc);
	}
	else
	{
		int status;
		waitpid(child_pid, &status, 0);
		if(WIFEXITED(status))
		{
			return WEXITSTATUS(status);
		}
		else
		{
			return -1;
		}
	}
}

int exec_wait(const std::string& path, std::string& stdout, ...)
{
	va_list vl;
	va_start(vl, stdout);
	
	std::vector<char*> args;
	args.push_back(const_cast<char*>(path.c_str()));
	
	while(true)
	{
		const char* p = va_arg(vl, const char*);
		if(p==NULL) break;
		args.push_back(const_cast<char*>(p));
	}
	va_end(vl);
	
	args.push_back(NULL);
	
	int pipefd[2];
	if (pipe(pipefd) == -1)
	{
		return -1;
	}
	
	pid_t child_pid = fork();
	
	if(child_pid==0)
	{
		environ = new char*[1];
		*environ=NULL;
		
		close(pipefd[0]);
		
		if(dup2(pipefd[1], 1)==-1)
		{
			return -1;
		}
		
		close(pipefd[1]);
		
		int rc = execvp(path.c_str(), args.data());
		exit(rc);
	}
	else
	{
		close(pipefd[1]);
		
		char buf[512];
		int r;
		while( (r=read(pipefd[0], buf, 512))>0)
		{
			stdout.insert(stdout.end(), buf, buf+r);
		}
		
		close(pipefd[0]);
	
		int status;
		waitpid(child_pid, &status, 0);
		if(WIFEXITED(status))
		{
			return WEXITSTATUS(status);
		}
		else
		{
			return -1;
		}
	}
}

bool chown_dir(const std::string& dir)
{
	passwd* user_info = getpwnam("urbackup");
	if(user_info)
	{
		int rc = chown(dir.c_str(), user_info->pw_uid, user_info->pw_gid);
		return rc!=-1;
	}
	return false;
}

std::string find_btrfs_cmd()
{
	static std::string btrfs_cmd;
	
	if(!btrfs_cmd.empty())
	{
		return btrfs_cmd;
	}
	
	if(exec_wait("btrfs", false, "--version", NULL)==0)
	{
		btrfs_cmd="btrfs";
		return btrfs_cmd;
	}
	else if(exec_wait("/sbin/btrfs", false, "--version", NULL)==0)
	{
		btrfs_cmd="/sbin/btrfs";
		return btrfs_cmd;
	}
	else if(exec_wait("/bin/btrfs", false, "--version", NULL)==0)
	{
		btrfs_cmd="/bin/btrfs";
		return btrfs_cmd;
	}
	else if(exec_wait("/usr/sbin/btrfs", false, "--version", NULL)==0)
	{
		btrfs_cmd="/usr/sbin/btrfs";
		return btrfs_cmd;
	}
	else if(exec_wait("/usr/bin/btrfs", false, "--version", NULL)==0)
	{
		btrfs_cmd="/usr/bin/btrfs";
		return btrfs_cmd;
	}
	else
	{
		btrfs_cmd="btrfs";
		return btrfs_cmd;
	}
}

std::string find_zfs_cmd()
{
	static std::string zfs_cmd;
	
	const char* const zfs_locations[] = { "zfs",
								   "/sbin/zfs", 
								   "/bin/zfs", 
								   "/usr/sbin/zfs",
								   "/usr/bin/zfs" };
								   
	for(size_t i=0;i<sizeof(zfs_locations)/sizeof(zfs_locations[0]);++i)
	{
		const char* location = zfs_locations[i];
		if(exec_wait(location, false, "version", NULL)==0)
		{
			zfs_cmd=location;
			return zfs_cmd;
		}
		if(exec_wait(location, false, "--version", NULL)==2)
		{
			zfs_cmd=location;
			return zfs_cmd;
		}
	}
	
	zfs_cmd="zfs";
	return zfs_cmd;
}

void zfs_elevate()
{
#ifdef __linux__
	if(setuid(0)!=0)
	{
		std::cout << "Cannot become root user (2)" << std::endl;
	}
#endif
}
#endif

bool create_subvolume(int mode, std::string subvolume_folder)
{
#ifdef _WIN32
	return os_create_dir(subvolume_folder);
#else
	if(mode==mode_btrfs)
	{
		int rc=exec_wait(find_btrfs_cmd(), true, "subvolume", "create", subvolume_folder.c_str(), NULL);
		chown_dir(subvolume_folder);
		return rc==0;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();
		int rc=exec_wait(find_zfs_cmd(), true, "create", "-p", subvolume_folder.c_str(), NULL);
		chown_dir(subvolume_folder);
		return rc==0;
	}
	return false;
#endif
}

bool get_mountpoint(int mode, std::string subvolume_folder)
{
#ifdef _WIN32
	std::cout << subvolume_folder << std::endl;
	return true;
#else
	if(mode==mode_btrfs)
	{
		std::cout << subvolume_folder << std::endl;
		return true;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();
		int rc=exec_wait(find_zfs_cmd(), true, "get", "-H", "-o", "value", "mountpoint", subvolume_folder.c_str(), NULL);
		return rc==0;
	}
	return false;
#endif
}

bool create_snapshot(int mode, std::string snapshot_src, std::string snapshot_dst)
{
#ifdef _WIN32
	return CopyFolder(widen(snapshot_src), widen(snapshot_dst));
#else
	if(mode==mode_btrfs)
	{
		int rc=exec_wait(find_btrfs_cmd(), true, "subvolume", "snapshot", snapshot_src.c_str(), snapshot_dst.c_str(), NULL);
		chown_dir(snapshot_dst);
		return rc==0;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();
		int rc=exec_wait(find_zfs_cmd(), true, "clone", (snapshot_src+"@ro").c_str(), snapshot_dst.c_str(), NULL);
		chown_dir(snapshot_dst);
		return rc==0;
	}
	return false;
#endif
}

bool is_subvolume(int mode, std::string subvolume_folder)
{
#ifdef _WIN32
	return true;
#else
	if(mode==mode_btrfs)
	{
		int rc=exec_wait(find_btrfs_cmd(), false, "subvolume", "list", subvolume_folder.c_str(), NULL);
		return rc==0;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();
		int rc=exec_wait(find_zfs_cmd(), false, "list", subvolume_folder.c_str(), NULL);
		return rc==0;
	}
	return false;
#endif
}

bool remove_subvolume(int mode, std::string subvolume_folder, bool quiet=false)
{
#ifdef _WIN32
	return os_remove_nonempty_dir(widen(subvolume_folder));
#else
	if(mode==mode_btrfs)
	{
		int compat_rc = exec_wait(find_btrfs_cmd(), false, "subvolume", "delete", "-c", NULL);
		
		if(compat_rc==1)
		{
			compat_rc = exec_wait("/bin/sh", false, "-c", (find_btrfs_cmd() +
				" subvolume delete -c 2>&1 | grep \"ERROR: error accessing '-c'\"").c_str(), NULL);
			if(compat_rc==0)
			{
				compat_rc=12;
			}
		}
		
		int rc;
		if(compat_rc==12)
		{
			rc=exec_wait(find_btrfs_cmd(), !quiet, "subvolume", "delete", subvolume_folder.c_str(), NULL);
		}
		else
		{
			rc=exec_wait(find_btrfs_cmd(), !quiet, "subvolume", "delete", "-c", subvolume_folder.c_str(), NULL);
		}
		return rc==0;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();

		// Ausführliche Dokumentation der ZFS-Fehlerursachen bei der Dataset-Löschung:
		// 1. EBUSY / Gemountete Datasets:
		//    "zfs destroy -r" ohne die Option "-f" scheitert, wenn das Dataset noch aktiv
		//    gemountet ist (oder überdeckte/shadowed Mountpoints vorliegen). Die Option "-f"
		//    erzwingt das Aushängen (force unmount) vor dem Löschen.
		//
		// 2. Fehlerannahme bezüglich "@ro"-Snapshots bei abhängigen Klonen:
		//    Der ursprüngliche Code ging fest davon aus, dass Klone ausschließlich an einem
		//    Snapshot namens "@ro" hängen (z.B. subvolume_folder + "@ro").
		//    Dies schlug in folgenden realen Szenarien fehl:
		//    a) Wenn der Snapshot in einem vorherigen partiellen Löschversuch bereits in
		//       "@" + rename_name (z.B. @260630-1435) umbenannt wurde.
		//    b) Wenn UrBackup oder Reparatur-Skripte Snapshots unter dem Zeitstempelnamen
		//       angelegt hatten (z.B. apo-ad1/260630-1435@260630-1435).
		//    In diesen Fällen lieferte "zfs get clones subvolume_folder@ro" den Fehler
		//    "dataset does not exist", wodurch die Abhängigkeitsauflösung sofort abbrach.
		//    Lösung: Alle vorhandenen Snapshots des Datasets via "zfs list -t snapshot -d 1"
		//    dynamisch ermitteln und auf Klone prüfen.
		//
		// 3. Klon-Promotion statt fehlerhafter Snapshot-Zerstörung:
		//    Mittels "zfs promote <clone>" wird die Klon-Hierarchie umgekehrt: Der Klon wird
		//    zum neuen Eigentümer des Snapshots und der Historie, sodass das Ziel-Dataset
		//    keine abhängigen Klone mehr besitzt und gefahrlos zerstört werden kann.
		//    Der alte Code versuchte nach dem Löschen des Datasets fälschlicherweise noch,
		//    den Snapshot auf dem beförderten Klon zu zerstören ("destroy clones[i]@rename_name").
		//    Wenn weitere Klone an diesem Snapshot hingen oder der Snapshot die Basis des
		//    Klons bildete, schlug dies fehl und ließ remove_subvolume fälschlicherweise
		//    einen Fehler zurückgeben, obwohl das Subvolume selbst bereits gelöscht war.

		int rc=exec_wait(find_zfs_cmd(), !quiet, "destroy", "-f", "-r", subvolume_folder.c_str(), NULL);
		if (rc == 0)
		{
			if (!quiet)
			{
				std::cout << "Destroyed subvol " << subvolume_folder << " successfully." << std::endl;
			}
			return true;
		}

		if (!quiet)
		{
			std::cout << "Checking " << subvolume_folder << " for dependencies..." << std::endl;
		}

		std::vector<std::string> snapshots;
		std::string snap_list_out;
		if (exec_wait(find_zfs_cmd(), snap_list_out, "list", "-H", "-o", "name", "-t", "snapshot", "-d", "1", subvolume_folder.c_str(), NULL) == 0)
		{
			std::vector<std::string> lines;
			Tokenize(snap_list_out, lines, "\n");
			for (size_t i = 0; i < lines.size(); ++i)
			{
				std::string s = trim(lines[i]);
				if (!s.empty())
				{
					snapshots.push_back(s);
				}
			}
		}

		const std::string ro_snap = subvolume_folder + "@ro";
		if (std::find(snapshots.begin(), snapshots.end(), ro_snap) == snapshots.end())
		{
			snapshots.push_back(ro_snap);
		}

		const std::string rename_name = ExtractFileName(subvolume_folder);
		bool had_dependencies = false;

		for (size_t s = 0; s < snapshots.size(); ++s)
		{
			const std::string& snap = snapshots[s];
			std::string clone_data;
			if (exec_wait(find_zfs_cmd(), clone_data, "get", "-H", "-o", "value", "clones", snap.c_str(), NULL) != 0)
			{
				continue;
			}

			std::vector<std::string> raw_clones;
			Tokenize(clone_data, raw_clones, ",");
			std::vector<std::string> clones;
			for (size_t i = 0; i < raw_clones.size(); ++i)
			{
				std::vector<std::string> sub_lines;
				Tokenize(raw_clones[i], sub_lines, "\n");
				for (size_t j = 0; j < sub_lines.size(); ++j)
				{
					std::string c = trim(sub_lines[j]);
					if (!c.empty() && c != "-")
					{
						clones.push_back(c);
					}
				}
			}

			if (clones.empty())
			{
				continue;
			}

			had_dependencies = true;
			if (!quiet)
			{
				std::cout << "Dependencies exist on snapshot " << snap << " (" << clones.size() << " clone(s))..." << std::endl;
			}

			// If the snapshot is named @ro, rename it so promoted clone won't collide with future @ro
			if (snap.size() >= 3 && snap.substr(snap.size() - 3) == "@ro")
			{
				const std::string target_snap = subvolume_folder + "@" + rename_name;
				if (exec_wait(find_zfs_cmd(), !quiet, "rename", snap.c_str(), target_snap.c_str(), NULL) != 0
					&& is_subvolume(mode, snap))
				{
					return false;
				}
			}

			// Promoting the clone transfers the snapshot to the clone and reverses the parent-clone relationship
			for (size_t i = 0; i < clones.size(); ++i)
			{
				if (!quiet)
				{
					std::cout << "Promoting " << clones[i] << "..." << std::endl;
				}
				if (exec_wait(find_zfs_cmd(), !quiet, "promote", clones[i].c_str(), NULL) == 0)
				{
					break;
				}
			}
		}

		if (!quiet)
		{
			std::cout << "Destroying subvol " << subvolume_folder << "..." << std::endl;
		}
		rc = exec_wait(find_zfs_cmd(), !quiet, "destroy", "-f", "-r", subvolume_folder.c_str(), NULL);
		if (rc == 0)
		{
			if (!quiet)
			{
				std::cout << "Destroyed subvol " << subvolume_folder << " successfully." << std::endl;
			}
			return true;
		}

		return false;
	}
	return false;
#endif
}

bool make_readonly(int mode, std::string subvolume_folder)
{
#ifdef _WIN32
	return false;
#else
	if(mode==mode_btrfs)
	{
		int rc=exec_wait(find_btrfs_cmd(), true, "property", "set", "-ts", subvolume_folder.c_str(), "ro", "true", NULL);
		return rc==0;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();
		int rc=exec_wait(find_zfs_cmd(), true, "snapshot", (subvolume_folder+"@ro").c_str(), NULL);
		return rc==0;
	}
	return false;
#endif
}

bool refresh_readonly(int mode, std::string subvolume_folder)
{
#ifdef _WIN32
	return false;
#else
	if(mode==mode_btrfs)
	{
		// Btrfs does not keep a separate @ro clone-origin snapshot in the same way as
		// ZFS. Reconcile-storage only needs this refresh step for ZFS datasets whose
		// sidecar metadata changed after import, so the btrfs path is intentionally a
		// no-op success.
		return true;
	}
	else if(mode==mode_zfs)
	{
		zfs_elevate();

		const std::string ro_snapshot = subvolume_folder+"@ro";
		const std::string refresh_name = ExtractFileName(subvolume_folder)+"-refresh";
		const std::string renamed_snapshot = subvolume_folder+"@"+refresh_name;

		// Replacing @ro after reconcile-storage changed .hashes keeps future clones
		// aligned with the repaired dataset state. If later backups already depend on
		// the old @ro, refresh follows the same rename/promote/destroy sequence used
		// during dataset removal before taking the new readonly snapshot.
		if(is_subvolume(mode, ro_snapshot))
		{
			std::string clone_data;
			int rc=exec_wait(find_zfs_cmd(), clone_data, "get", "-H", "-o", "value", "clones", ro_snapshot.c_str(), NULL);
			if(rc!=0)
			{
				return false;
			}

			std::vector<std::string> clones;
			Tokenize(clone_data, clones, "\n");
			clones.erase(std::remove_if(clones.begin(), clones.end(),
				[](const std::string& v) { return trim(v).empty() || trim(v)=="-"; }), clones.end());

			if (!clones.empty())
			{
				if (exec_wait(find_zfs_cmd(), true, "rename", ro_snapshot.c_str(), renamed_snapshot.c_str(), NULL)!=0
					&& is_subvolume(mode, ro_snapshot))
				{
					return false;
				}

				for(size_t i=0;i<clones.size();++i)
				{
					if(exec_wait(find_zfs_cmd(), true, "promote", clones[i].c_str(), NULL)!=0)
					{
						return false;
					}
				}

				// zfs promote reverses the clone relationship: the renamed snapshot is
				// then the origin of the former parent dataset. It must remain in place;
				// destroying it would fail on that dependent clone and abort the metadata
				// refresh before the repaired dataset receives its new @ro snapshot.
			}
			else if(exec_wait(find_zfs_cmd(), true, "destroy", ro_snapshot.c_str(), NULL)!=0)
			{
				return false;
			}
		}

		return make_readonly(mode, subvolume_folder);
	}
	return false;
#endif
}

int zfs_test()
{
	std::cout << "Testing for zfs..." << std::endl;
				
	if(getBackupfolderPath(mode_zfs).empty())
	{
		std::cout << "TEST FAILED: Dataset is not set via /etc/urbackup/dataset" << std::endl;
		return 1;
	}
	
	std::string clientdir=getBackupfolderPath(mode_zfs)+os_file_sep()+"testA54hj5luZtlorr494";
	remove_subvolume(mode_zfs, clientdir, true);
	
	if(create_subvolume(mode_zfs, clientdir)
		&& remove_subvolume(mode_zfs, clientdir) )
	{
		std::cout << "ZFS TEST OK" << std::endl;
		
		clientdir=getBackupfolderPath(mode_zfs_file)+os_file_sep()+"testA54hj5luZtlorr494";
		remove_subvolume(mode_zfs, clientdir, true);
		
		if(getBackupfolderPath(mode_zfs_file).empty())
		{
			return 10 + mode_zfs;
		}
		else if(create_subvolume(mode_zfs, clientdir)
			&& remove_subvolume(mode_zfs, clientdir))
		{
			return 10 + mode_zfs_file;
		}
	
		return 10 + mode_zfs;
	}
	else
	{
		std::cout << "TEST FAILED: Creating test zfs volume \"" << clientdir << "\" failed" << std::endl;
	}
	return 1;
}

int main(int argc, char *argv[])
{
	if(argc<2)
	{
		std::cout << "Not enough parameters" << std::endl;
		return 1;
	}
	
	std::string cmd;
	int mode = 0;
	if((std::string)argv[1]!="test")
	{
		if(argc<3)
		{
			std::cout << "Not enough parameters" << std::endl;
			return 1;
		}
		cmd=argv[2];
		mode=atoi(argv[1]);
	}
	else
	{
		cmd=argv[1];
	}

	std::string backupfolder=getBackupfolderPath(mode);
	
	if(backupfolder.empty())
	{	
		if(mode==mode_btrfs)
		{
			std::cout << "Backupfolder not set" << std::endl;
		}
		else if(mode==mode_zfs)
		{
			std::cout << "ZFS image dataset not set" << std::endl;
		}
		else if(mode==mode_zfs_file)
		{
			std::cout << "ZFS file dataset not set" << std::endl;
		}
		else
		{
			std::cout << "Unknown mode: " << mode << std::endl;
		}
		return 1;
	}
	
	if(cmd!="test" && mode==mode_zfs_file)
	{
		mode=mode_zfs;
	}
	
#ifndef _WIN32
	if(seteuid(0)!=0)
	{
		std::cout << "Cannot become root user" << std::endl;
		return 1;
	}
#endif

	if(cmd=="create")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for create" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);

		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);
		
		return create_subvolume(mode, subvolume_folder)?0:1;
	}
	else if(cmd=="mountpoint")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for mountpoint" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);

		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);
		
		return get_mountpoint(mode, subvolume_folder)?0:1;
	}
	else if(cmd=="snapshot")
	{
		if(argc<6)
		{
			std::cout << "Not enough parameters for snapshot" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string src_name=handleFilename(argv[4], mode);
		std::string dst_name=handleFilename(argv[5], mode);

		std::string subvolume_src_folder=get_subvolume_folder(mode, backupfolder, clientname, src_name);
		std::string subvolume_dst_folder=get_subvolume_folder(mode, backupfolder, clientname, dst_name);

		return create_snapshot(mode, subvolume_src_folder, subvolume_dst_folder)?0:1;
	}
	else if(cmd=="remove")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for remove" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);

		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);
		
		return remove_subvolume(mode, subvolume_folder)?0:1;
	}
	else if(cmd=="test")
	{
		std::cout << "Testing for btrfs..." << std::endl;
		std::string clientdir=backupfolder+os_file_sep()+"testA54hj5luZtlorr494";
		
		bool create_dir_rc=os_create_dir(clientdir);
		if(!create_dir_rc)
		{	
			remove_subvolume(mode_zfs, clientdir, true);
			remove_subvolume(mode_btrfs, clientdir+os_file_sep()+"A", true);
			remove_subvolume(mode_btrfs, clientdir+os_file_sep()+"B", true);
			os_remove_dir(clientdir);
		}
		create_dir_rc = create_dir_rc || os_create_dir(clientdir);
		if(create_dir_rc)
		{	
			if(!create_subvolume(mode_btrfs, clientdir+os_file_sep()+"A") )
			{
				std::cout << "TEST FAILED: Creating test btrfs subvolume failed" << std::endl;
				os_remove_dir(clientdir);
				
				return zfs_test();
			}
			
			bool suc=true;

			if(!create_snapshot(mode_btrfs, clientdir+os_file_sep()+"A", clientdir+os_file_sep()+"B") )
			{
				std::cout << "TEST FAILED: Creating test snapshot failed" << std::endl;
				suc=false;
			}
			
			if(suc)
			{			
				writestring("test", clientdir+os_file_sep()+"A"+os_file_sep()+"test");
				
				if(!os_create_hardlink(clientdir+os_file_sep()+"B"+os_file_sep()+"test", clientdir+os_file_sep()+"A"+os_file_sep()+"test", true, NULL))
				{
					std::cout << "TEST FAILED: Creating cross sub-volume reflink failed. Need Linux kernel >= 3.6." << std::endl;
					suc=false;
				}
				else
				{
					if(getFile(clientdir+os_file_sep()+"B"+os_file_sep()+"test")!="test")
					{
						std::cout << "TEST FAILED: Cannot read reflinked file" << std::endl;
						suc=false;
					}
				}
			}

			if(!remove_subvolume(mode_btrfs, clientdir+os_file_sep()+"A") )
			{
				std::cout << "TEST FAILED: Removing subvolume A failed" << std::endl;
				suc=false;
			}

			if(!remove_subvolume(mode_btrfs, clientdir+os_file_sep()+"B") )
			{
				std::cout << "TEST FAILED: Removing subvolume B failed" << std::endl;
				suc=false;
			}

			if(!os_remove_dir(clientdir))
			{
				std::cout << "TEST FAILED: Removing test clientdir failed" << std::endl;
				return 1;
			}
			
			if(!suc)
			{
				return 1;
			}
		}
		else
		{
			std::cout << "TEST FAILED: Creating test clientdir \"" << clientdir << "\" failed" << std::endl;
						
			return zfs_test();
		}
		std::cout << "BTRFS TEST OK" << std::endl;
		return 10 + mode_btrfs;
	}
	else if(cmd=="issubvolume")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for issubvolume" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);

		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);
		
		return is_subvolume(mode, subvolume_folder)?0:1;
	}
	else if(cmd=="makereadonly")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for makereadonly" << std::endl;
			return 1;
		}
		
		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);

		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);
		
		return make_readonly(mode, subvolume_folder)?0:1;
	}
	else if(cmd=="hasreadonly")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for hasreadonly" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);
		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);

		// Reconcile-storage uses this after an interrupted refresh to distinguish a
		// valid existing @ro origin from a backup that needs only snapshot repair.
		return is_subvolume(mode, subvolume_folder+"@ro") ? 0 : 1;
	}
	else if(cmd=="refreshreadonly")
	{
		if(argc<5)
		{
			std::cout << "Not enough parameters for refreshreadonly" << std::endl;
			return 1;
		}

		std::string clientname=handleFilename(argv[3], mode);
		std::string name=handleFilename(argv[4], mode);

		std::string subvolume_folder=get_subvolume_folder(mode, backupfolder, clientname, name);

		return refresh_readonly(mode, subvolume_folder)?0:1;
	}
	else
	{
		std::cout << "Command not found" << std::endl;
		return 1;
	}
}
