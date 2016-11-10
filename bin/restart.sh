#!/usr/bin/env bash

function command_exists () {
	hash "$1" &> /dev/null
}

worker_id=""

while [ $# -gt 0 ]; do
	case "$1" in
		--id=*)
			worker_id="${1#*=}"
		;;
	esac
	shift
done

if command_exists "forever" ; then
	if [ $worker_id ]; then
		forever restart ${worker_id}
	else
		forever restartall
	fi
else
	echo "Unable to run script. Please install 'forever' npm package globally."
	exit 1
fi
