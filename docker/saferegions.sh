#!/usr/bin/env bash


usage="Call -h for help"

HBASEMODE=-1
function help () {
	echo "Command used to configure hbase-site.xml and start hbase"
	echo "Available arguments: "
	echo "-s Starts HBase in standalone mode"
	echo "-p Starts HBase in pseudo distributed mode"
	echo "-d Starts HBase in distributed mode"
	echo "playerId sets the id of the player (0,1,2)."
	echo "relayPort sets the port that the relay will listen to."
	echo "firstTargetHost sets the first target host that the relay must connect."
	echo "firstTargetPort sets the first target port that the relay must connect to."
	echo "secondTargetHost sets the second target host that the relay must connect."
	echo "seconTargetPort sets the second target port that the relay must connect to."
	#The following two parameters are required when the cluster is executed 
	#on a local host with three docker instances. This allow us to
	#expose the ports on the host.
	echo "masterPort sets the port for the client to connect to the Master."
	echo "zookeeperPort sets the port for the client to connect to the zookeeper."
	echo "batchSize sets the size of messages exchanged in a protocol batch."
	echo "randomPool sets the size for a pool of random values."
}

: "${HBASE_HOME:?Need to set HBASE_HOME non-empty}"

while getopts ":spdh" opt; do
	case $opt in
		s ) HBASEMODE=0
			;;
		p ) echo "Not Implemented"
			exit 1
			;;
		d ) echo "Not Implemented"
			exit 1
			;;
		h ) help
			exit 1
			;;
		\? ) echo $usage
			exit 1
	esac
done
shift $(($OPTIND - 1))

playerId=$1		
relayPort=$2
firstTargetHost=$3
firstTargetPort=$4
secondTargetHost=$5
secondTargetPort=$6
masterPort=$7
zookeeperPort=$8
batchSize=$9
randomPool=$10


function standalone () {
	# This paths are aligned with the Dockerfile. could be parametrized
	cp standalone-template.xml "$HBASE_HOME/conf/hbase-site.xml"
	cd $HBASE_HOME/conf

	sed -i "s/{PLAYERID}/$playerId/g" hbase-site.xml
	sed -i "s/{RELAYPORT}/$relayPort/g" hbase-site.xml
	sed -i "s/{FIRSTTARGETHOST}/$firstTargetHost/g" hbase-site.xml
	sed -i "s/{FIRSTTARGETPORT}/$firstTargetPort/g" hbase-site.xml
	sed -i "s/{SECONDTARGETHOST}/$secondTargetHost/g" hbase-site.xml
	sed -i "s/{SECONDTARGETPORT}/$secondTargetPort/g" hbase-site.xml
	sed -i "s/{MASTERPORT}/$masterPort/g" hbase-site.xml
	sed -i "s/{ZOOKEEPERPORT}/$zookeeperPort/g" hbase-site.xml
	sed -i "s/{BATCHSIZE}/$batchSize/g" hbase-site.xml
	sed -i "s/{RANDOMPOOL}/$randomPool/g" hbase-site.xml

	cd ../bin
	./start-hbase.sh
	cd ..
	tail -f /usr/local/hbase/hbase-0.98.24-hadoop2/bin/log/*
}


if(($HBASEMODE==0));then
	standalone
fi
