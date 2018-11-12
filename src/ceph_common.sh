#!/bin/sh

CCONF="$BINDIR/ceph-conf"

default_conf=$ETCDIR"/ceph.conf"
conf=$default_conf

hostname=`hostname -s`

wlog() {
    # Syntax: "wlog <name> <err_lvl> <log_msg> [print_trace]"
    # err_lvl should be INFO, WARN, ERROR or DEBUG
    #  o INFO - state transitions & normal messages
    #  o WARN - unexpected events (i.e. processes marked as down)
    #  o ERROR - hang messages and unexpected errors
    #  o DEBUG - print debug messages
    if [ -z "$LOG_FILE" ] || [ "$LOG_LEVEL" != "DEBUG" ] && [ "$2" = "DEBUG" ]; then
        # hide messages
        return
    fi

    local head="$(date "+%Y-%m-%d %H:%M:%S.%3N") $0 $1"
    echo "$head $2: $3" >> $LOG_FILE
    if [ "$4" = "print_trace" ]; then
        # Print out the stack trace
        if [ ${#FUNCNAME[@]} -gt 1 ]; then
            echo "$head   Call trace:" >> $LOG_FILE
            for ((i=0;i<${#FUNCNAME[@]}-1;i++)); do
                echo "$head     $i: ${BASH_SOURCE[$i+1]}:${BASH_LINENO[$i]} ${FUNCNAME[$i]}(...)" >> $LOG_FILE
            done
        fi
    fi
}

CEPH_FAILURE=""
execute_ceph_cmd() {
    # execute a comand and in case it timeouts mark ceph as failed
    local ret=$1
    local name=$2
    local cmd=$3
    local cmd="timeout $WAIT_FOR_CMD $cmd"
    set -o pipefail
    eval "$cmd &>$DATA_PATH/.ceph_cmd_out"
    errcode=$?
    set +o pipefail
    if [ -z "$output" ] && [ $errcode -eq 124 ]; then  # 'timeout' returns 124 when timing out
        wlog $name "WARN" "Ceph failed to respond in ${WAIT_FOR_CMD}s when running: $cmd"
        CEPH_FAILURE="true"
        echo ""; return 1
    fi
    output=$(cat $DATA_PATH/.ceph_cmd_out)
    if [ -z "$output" ] || [ $errcode -ne 0 ]; then
        wlog $name "WARN" "Error executing: $cmd errorcode: $errcode output: $output"
        echo ""; return 1
    fi
    eval "$ret=\"$output\""; return $errcode
}

verify_conf() {
    # fetch conf?
    if [ -x "$ETCDIR/fetch_config" ] && [ "$conf" = "$default_conf" ]; then
	conf="/tmp/fetched.ceph.conf.$$"
	echo "[$ETCDIR/fetch_config $conf]"
	if $ETCDIR/fetch_config $conf && [ -e $conf ]; then true ; else
	    echo "$0: failed to fetch config with '$ETCDIR/fetch_config $conf'"
	    exit 1
	fi
	# yay!
    else
        # make sure ceph.conf exists
	if [ ! -e $conf ]; then
	    if [ "$conf" = "$default_conf" ]; then
		echo "$0: ceph conf $conf not found; system is not configured."
		exit 0
	    fi
	    echo "$0: ceph conf $conf not found!"
	    usage_exit
	fi
    fi
}

check_host() {
    # what host is this daemon assigned to?
    host=`$CCONF -c $conf -n $type.$id host`
    if [ "$host" = "localhost" ]; then
	echo "$0: use a proper short hostname (hostname -s), not 'localhost', in $conf section $type.$id; skipping entry"
	return 1
    fi
    if expr match "$host" '.*\.' > /dev/null 2>&1; then
	echo "$0: $conf section $type.$id"
	echo "contains host=$host, which contains dots; this is probably wrong"
	echo "It must match the result of hostname -s"
    fi
    ssh=""
    rootssh=""
    sshdir=$PWD
    get_conf user "" "user"

    #echo host for $name is $host, i am $hostname

    cluster=$1
    if [ -e "/var/lib/ceph/$type/$cluster-$id/upstart" ]; then
	return 1
    fi

    # sysvinit managed instance in standard location?
    # 'sysvinit' file is required to start the daemon.
    # For osd daemon on a storage host, this file is created during 'ceph-disk activate-all',
    # executed from '/etc/init.d/ceph'.
    # It is possible to have transitory disk I/O errors causing activate to fail
    # and not create 'sysvinit' file. Also, all osd daemons listed here are local.
    # Give pmon a chance to restart the osd daemon.
    # If daemon type is 'osd', skip checking for 'sysvinit' file presence.
    if [ -e "/var/lib/ceph/$type/$cluster-$id/sysvinit" ] || [ "$type" = "osd" ]; then
	host="$hostname"
	echo "=== $type.$id === "
	return 0
    fi

    # ignore all sections without 'host' defined
    if [ -z "$host" ]; then
	return 1
    fi

    if [ "$host" != "$hostname" ]; then
	# skip, unless we're starting remote daemons too
	if [ $allhosts -eq 0 ]; then
	    return 1
	fi

	# we'll need to ssh into that host
	if [ -z "$user" ]; then
	    ssh="ssh $host"
	else
	    ssh="ssh $user@$host"
	fi
	rootssh="ssh root@$host"
	get_conf sshdir "$sshdir" "ssh path"
    fi

    echo "=== $type.$id === "

    return 0
}

do_cmd() {
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "$user" ] || [ -z "$user" ]; then
	    bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && exit 1; }
	else
	    sudo su $user -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && exit 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $ssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1\""
	$ssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1" || { [ -z "$3" ] && echo "failed: '$ssh $1'" && exit 1; }
    fi
}

do_cmd_okfail() {
    ERR=0
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "$user" ] || [ -z "$user" ]; then
	    bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && ERR=1 && return 1; }
	else
	    sudo su $user -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && ERR=1 && return 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $ssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1\""
	$ssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1" || { [ -z "$3" ] && echo "failed: '$ssh $1'" && ERR=1 && return 1; }
    fi
    return 0
}

do_root_cmd() {
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "root" ]; then
	    bash -c "$1" || { echo "failed: '$1'" ; exit 1; }
	else
	    sudo bash -c "$1" || { echo "failed: '$1'" ; exit 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $rootssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi ; cd $sshdir ; ulimit -c unlimited ; $1\""
	$rootssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi ; cd $sshdir; ulimit -c unlimited ; $1" || { echo "failed: '$rootssh $1'" ; exit 1; }
    fi
}

do_root_cmd_okfail() {
    ERR=0
    if [ -z "$ssh" ]; then
	[ $verbose -eq 1 ] && echo "--- $host# $1"
	ulimit -c unlimited
	whoami=`whoami`
	if [ "$whoami" = "root" ]; then
	    bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && ERR=1 && return 1; }
	else
	    sudo bash -c "$1" || { [ -z "$3" ] && echo "failed: '$1'" && ERR=1 && return 1; }
	fi
    else
	[ $verbose -eq 1 ] && echo "--- $rootssh $2 \"if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1\""
	$rootssh $2 "if [ ! -d $sshdir ]; then mkdir -p $sshdir; fi; cd $sshdir ; ulimit -c unlimited ; $1" || { [ -z "$3" ] && echo "failed: '$rootssh $1'" && ERR=1 && return 1; }
    fi
    return 0
}

get_local_daemon_list() {
    type=$1
    if [ -d "/var/lib/ceph/$type" ]; then
	for p in `find -L /var/lib/ceph/$type -mindepth 1 -maxdepth 1 -type d`; do
	    i=`basename $p` 

	    # 'sysvinit' file is required to start a ceph daemon.
	    # For osd daemon on a storage host, this file is created during 'ceph-disk activate-all',
	    # executed from '/etc/init.d/ceph'.
	    # It is possible to have transitory disk I/O errors causing activate to fail
	    # and not create 'sysvinit' file. Give pmon a chance to restart the osd daemon.
	    # For other ceph daemons, 'sysvinit' file creation is triggered differently
	    # (e.g. puppet creates it for monitors).
	    # If daemon type is 'osd', skip checking for 'sysvinit' file presence.
	    id=`echo $i | sed 's/[^-]*-//'`
	    daemon="$type.$id"
	    if [ ! -e "/var/lib/ceph/$type/$i/sysvinit" ] && [ "$command" = "start" ] && [ "$id" != "lost+found" ]; then
		    wlog "$daemon" "WARN" "/var/lib/ceph/$type/$i/sysvinit file is missing"
	    fi
	    if [ -e "/var/lib/ceph/$type/$i/sysvinit" ] || [ "$type" = "osd" ]; then
		    local="$local $daemon"
	    fi
	done
    fi
}

get_local_name_list() {
    # enumerate local directories
    local=""
    get_local_daemon_list "mon"
    get_local_daemon_list "osd"
    get_local_daemon_list "mds"
    get_local_daemon_list "mgr"
}

get_name_list() {
    orig="$*"

    # extract list of monitors, mdss, osds, mgrs defined in startup.conf
    tmp=$(mktemp /tmp/ceph.XXXXXXX)
    echo $local >> $tmp
    $CCONF -c $conf -l mon | egrep -v '^mon$' >> $tmp || true
    $CCONF -c $conf -l mds | egrep -v '^mds$' >> $tmp || true
    $CCONF -c $conf -l mgr | egrep -v '^mgr$' >> $tmp || true
    $CCONF -c $conf -l osd | egrep -v '^osd$' >> $tmp || true
    allconf=`cat $tmp | xargs -n1 | sort -u | xargs`
    rm $tmp

    if [ -z "$orig" ]; then
	what="$allconf"
	return
    fi

    what=""
    for f in $orig; do
	type=`echo $f | cut -c 1-3`   # e.g. 'mon', if $item is 'mon1'
	id=`echo $f | cut -c 4- | sed 's/\\.//'`
	case $f in
	    mon | osd | mds | mgr)
		for d in $allconf; do
		    if echo $d | grep -q ^$type; then
			what="$what $d"
		    fi
		done
		;;
	    *)
		if ! echo " " $allconf $local " " | egrep -q "( $type$id | $type.$id )"; then
		    echo "$0: $type.$id not found ($conf defines" $allconf", /var/lib/ceph defines" $local")"
		    exit 1
		fi
		what="$what $f"
		;;
	esac
    done
}

get_conf() {
	var=$1
	def=$2
	key=$3
	shift; shift; shift

	if [ -z "$1" ]; then
	    [ "$verbose" -eq 1 ] && echo "$CCONF -c $conf -n $type.$id \"$key\""
	    eval "$var=\"`$CCONF -c $conf -n $type.$id \"$key\" || printf \"$def\"`\""
	else
	    [ "$verbose" -eq 1 ] && echo "$CCONF -c $conf -s $1 \"$key\""
	    eval "$var=\"`$CCONF -c $conf -s $1 \"$key\" || eval printf \"$def\"`\""
	fi
}

get_conf_bool() {
	get_conf "$@"

	eval "val=$"$1
	[ "$val" = "0" ] && export $1=0
	[ "$val" = "false" ] && export $1=0
	[ "$val" = "1" ] && export $1=1
	[ "$val" = "true" ] && export $1=1
}

