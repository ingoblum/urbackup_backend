#!/bin/bash

cbuild=""
CPWD=`pwd`

copy_cond()
{
	R_CMP=`cmp $1 $2`
	if [ "xR_CMP" != "x0" ]
	then
		cp $1 $2
	fi
}

switch_m_build()
{
	if test -e defaults_$cbuild
	then
		copy_cond defaults_$cbuild defaults
		copy_cond init.d_$cbuild init.d
	fi
}

switch_build()
{
	if test -e Makefile.am_$cbuild
	then
		pwd
		copy_cond Makefile.am_$cbuild Makefile.am
		copy_cond configure.ac_$cbuild configure.ac
		if test -e BUILDID
		then
		    BUILDID=`cat BUILDID`
		    BUILDID=$((BUILDID +1))
		    sed -i "s/BUILDID/$BUILDID/g" configure.ac
		    echo $BUILDID > BUILDID
		else
		    sed -i "s/BUILDID/0/g" configure.ac
		fi
	fi
	if [[ "x$cbuild" == "xserver" ]] && [ -f urbackupserver/www/index.htm ]; then
		update_www_version
	fi
}

update_www_version()
{
	local base_ver="2.5.37"
	local git_hash="unknown" git_date="" git_msg=""
	if command -v git >/dev/null 2>&1 && [ -d .git ]; then
		git_hash=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
		git_date=$(git log -1 --format=%cd --date=short 2>/dev/null || echo "")
		git_msg=$(git log -1 --format=%s 2>/dev/null || echo "")
	fi

	python3 - <<PYEOF 2>/dev/null || true
import re, html

base_ver = "${base_ver}"
git_hash = "${git_hash}"
git_date = "${git_date}"
git_msg = html.escape("""${git_msg}""", quote=True)

with open('urbackupserver/www/index.htm', 'r', encoding='utf-8') as f:
    content = f.read()

full_ver = f"{base_ver} (git-{git_hash}, {git_date}: {git_msg})"
span_html = f'<span id="urbackup_version" style="cursor:help" title="Commit: {git_hash} ({git_date})&#10;Message: {git_msg}">UrBackup {base_ver} ({git_hash}, {git_date})</span>'

content = re.sub(r'<div style="display:none" id="server_version_full">.*?</div>', f'<div style="display:none" id="server_version_full">{full_ver}</div>', content)
content = re.sub(r'<span class="pull-right">[\s\S]*?<a href="javascript:\s*aboutUrBackup\(\)"', f'<span class="pull-right">\n\t\t\t\t\t\t\t{span_html} | <a href="javascript: aboutUrBackup()"', content)

with open('urbackupserver/www/index.htm', 'w', encoding='utf-8') as f:
    f.write(content)
PYEOF
}

switch()
{
	switch_build
	switch_m_build
}

clean_build()
{
	if test -e "$CPWD/curr_build"
	then
		c=`cat $CPWD/curr_build`
		echo "Last build: $c"
		if [[ "x$c" == "xserver" ]] && [[ "x$cbuild" == "xclient" ]]
		then
			make clean
		fi
		if [[ "x$c" == "xclient" ]] && [[ "x$cbuild" == "xserver" ]]
		then
			make clean
		fi
	fi
	echo "$cbuild" > $CPWD/curr_build
}

if [[ "x$1" == "xserver" ]]
then
	cbuild="server"	
elif [[ "x$1" == "xclient" ]]
then
	cbuild="client"
else
	echo "No valid build environment. Available: client, server"
	exit 0
fi

switch
clean_build
echo "Switched to $cbuild"
