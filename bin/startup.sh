#!/usr/bin/env bash

function command_exists() {
	hash "$1" &> /dev/null
}

declare -A env_vars

index=0
type=""
TP_SCKT_PORT=""
TP_SSL=""
TP_KAFKA_HOST=""
TP_KFK_PORT=""
TP_KFK_CLIENT=""
TP_REDIS_HOST=""
TP_REDIS_PORT=""
TP_REDISCACHE=""
TP_REDISCACHE_PORT=""
TP_MAIN_DB=""
TP_ES_HOST=""
TP_ES_PORT=""
TP_AMQP_HOST=""
TP_AMQP_USER=""
TP_AMQP_PASSWORD=""

env_vars_string=""

worker_types=("aggregation" "write" "transport_manager" "android_transport" "ios_transport" "sockets_transport")

while [ $# -gt 0 ]; do
	case "$1" in
		--t=*)
			type="${1#*=}"
		;;
		--i=*)
			index="${1#*=}"
		;;
		--TP_SCKT_PORT=*)
			TP_SCKT_PORT="${1#*=}"
			env_vars_string+="TP_SCKT_PORT=$TP_SCKT_PORT "
		;;
		--TP_SCKT_SSL=*)
			TP_SCKT_SSL="${1#*=}"
			env_vars_string+="TP_SSL=$TP_SCKT_SSL "
		;;
		--TP_KAFKA_HOST=*)
			TP_KAFKA_HOST="${1#*=}"
			env_vars_string+="TP_KAFKA_HOST=$TP_KAFKA_HOST "
		;;
		--TP_KFK_PORT=*)
			TP_KFK_PORT="${1#*=}"
			env_vars_string+="TP_KFK_PORT=$TP_KFK_PORT "
		;;
		--TP_KFK_CLIENT=*)
			TP_KFK_CLIENT="${1#*=}"
			env_vars_string+="TP_KFK_CLIENT=$TP_KFK_CLIENT "
		;;
		--TP_REDIS_HOST=*)
			TP_REDIS_HOST="${1#*=}"
			env_vars_string+="TP_REDIS_HOST=$TP_REDIS_HOST "
		;;
		--TP_REDIS_PORT=*)
			TP_REDIS_PORT="${1#*=}"
			env_vars_string+="TP_REDIS_PORT=$TP_REDIS_PORT "
		;;
		--TP_REDISCACHE=*)
			TP_REDISCACHE="${1#*=}"
			env_vars_string+="TP_REDISCACHE=$TP_REDISCACHE "
		;;
		--TP_REDISCACHE_PORT=*)
			TP_REDISCACHE_PORT="${1#*=}"
			env_vars_string+="TP_REDISCACHE_PORT=$TP_REDISCACHE_PORT "
		;;
		--TP_MAIN_DB=*)
			TP_MAIN_DB="${1#*=}"
			env_vars_string+="TP_MAIN_DB=$TP_MAIN_DB "
		;;
		--TP_ES_HOST=*)
			TP_ES_HOST="${1#*=}"
			env_vars_string+="TP_ES_HOST=$TP_ES_HOST "
		;;
		--TP_ES_PORT=*)
			TP_ES_PORT="${1#*=}"
			env_vars_string+="TP_ES_PORT=$TP_ES_PORT "
		;;
		--TP_AMQP_HOST=*)
			TP_AMQP_HOST="${1#*=}"
			env_vars_string+="TP_AMQP_HOST=$TP_AMQP_HOST "
		;;
		--TP_AMQP_USER=*)
			TP_AMQP_USER="${1#*=}"
			env_vars_string+="TP_AMQP_USER=$TP_AMQP_USER "
		;;
		--TP_AMQP_PASSWORD=*)
			TP_AMQP_PASSWORD="${1#*=}"
			env_vars_string+="TP_AMQP_PASSWORD=$TP_AMQP_PASSWORD "
	esac
	shift
done

if command_exists "forever" ; then
	if [ $TP_SCKT_PORT ]; then
		sleep 0
	else
		if [ $TP_SCKT_SSL ]; then
			TP_SCKT_PORT=443
		else
			TP_SCKT_PORT=3000
		fi
	fi

	if [[ $TP_SCKT_SSL == "1" && $TP_SCKT_PORT -lt 1023 && "$(whoami)" != "root" ]]; then
		echo "ERROR: You must run this command as root (or with sudo)"
		exit 3
	else
		total_ram=$(cat /proc/meminfo | grep MemTotal | awk '{ print $2 }')

		if command_exists "bc"; then
			total_ram=$(printf "%d" $(bc <<< "scale=2; ${total_ram}/1024*(3/4)") 2>/dev/null)
		else
			total_ram=$((total_ram/1024))
		fi

		eval "${env_vars_string} forever start --append --uid \"${type}${index}\" --colors -o ./logs/${type}${index}.out -e ./logs/${type}${index}.err -c \"node --nouse-idle-notification --max-old-space-size=${total_ram}\" ./index.js -t ${type} -i ${index}"
	fi
else
	echo "Unable to run script. Please install 'forever' npm package globally."
	exit 4
fi
