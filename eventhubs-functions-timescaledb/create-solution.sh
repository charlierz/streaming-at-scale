#!/bin/bash

# Strict mode, fail on any error
set -euo pipefail

on_error() {
    set +e
    echo "There was an error, execution halted" >&2
    echo "Error at line $1"
    exit 1
}

trap 'on_error $LINENO' ERR

export UBUNTU_PASSWORD='inspiron911'

export PREFIX=''
export LOCATION="eastus"
export TESTTYPE="1"
export PROC_FUNCTION="Test0"
export STEPS="D"
export GENERATOR_MODE="eventhubs"

usage() { 
    echo "Usage: $0 -d <deployment-name> [-s <steps>] [-t <test-type>] [-f <function-type>] [-l <location>] [-g <generator-mode>]"
    echo "-s: specify which steps should be executed. Default=$STEPS"
    echo "    Possible values:"
    echo "      C=COMMON"
    echo "      I=INGESTION"
    echo "      D=DATABASE"
    echo "      P=PROCESSING"
    echo "      T=TEST clients"
    echo "      M=METRICS reporting"
    echo "-t: test 1,5,10 thousands msgs/sec. Default=$TESTTYPE"
    echo "-f: function to test. Default=$PROC_FUNCTION"
    echo "-l: where to create the resources. Default=$LOCATION"
    echo "-g: Protocol used by event generator: 'eventhubs' or 'kafka'. Default=$GENERATOR_MODE"
    exit 1; 
}

# Initialize parameters specified from command line
while getopts ":d:s:t:l:f:g:" arg; do
	case "${arg}" in
		d)
			PREFIX=${OPTARG}
			;;
		s)
			STEPS=${OPTARG}
			;;
		t)
			TESTTYPE=${OPTARG}
			;;
		l)
			LOCATION=${OPTARG}
			;;
                f)
			PROC_FUNCTION=${OPTARG}
			;;
                g)
			GENERATOR_MODE=${OPTARG}
			;;
		esac
done
shift $((OPTIND-1))

if [[ -z "$PREFIX" ]]; then
	echo "Enter a name for this deployment."
	usage
fi

if [ "$TESTTYPE" == "1" ]; then
    export EVENTHUB_PARTITIONS=4
    export EVENTHUB_CAPACITY=4
    export PROC_FUNCTION_SKU=P2v2
    export PROC_FUNCTION_WORKERS=1
    export POSTGRESQL_SKU=GP_Gen5_2
    export POSTGRESQL_STORAGE_SIZE=5120
    export SIMULATOR_INSTANCES=2
fi

# last checks and variables setup
if [ -z "${SIMULATOR_INSTANCES+x}" ]; then
    usage
fi

case $PROC_FUNCTION in
    Test0)
        ;;
    Test1)
        ;;
    *)
        echo "PROC_FUNCTION (option '-f') must be set to 'Test0' or 'Test1'"
        echo "Check documentation on GitHub"
        usage
esac

export RESOURCE_GROUP=$PREFIX

# remove log.txt if exists
rm -f log.txt

echo "Checking pre-requisites..."

source ../assert/has-local-az.sh
source ../assert/has-local-jq.sh
source ../assert/has-local-zip.sh
source ../assert/has-local-dotnet.sh

echo
echo "Streaming at Scale with Azure Functions and CosmosDB"
echo "===================================================="
echo

echo "Steps to be executed: $STEPS"
echo

echo "Configuration: "
echo ". Resource Group  => $RESOURCE_GROUP"
echo ". Region          => $LOCATION"
echo ". EventHubs       => TU: $EVENTHUB_CAPACITY, Partitions: $EVENTHUB_PARTITIONS"
echo ". Function        => Name: $PROC_FUNCTION, SKU: $PROC_FUNCTION_SKU, Workers: $PROC_FUNCTION_WORKERS"
echo ". TimescaleDB     => SKU: $POSTGRESQL_SKU, Storage: $POSTGRESQL_STORAGE_SIZE"
echo ". Simulators      => $SIMULATOR_INSTANCES"
echo

echo "Deployment started..."
echo

echo "***** [C] setting up COMMON resources"

    export AZURE_STORAGE_ACCOUNT=$PREFIX"storage"

    RUN=`echo $STEPS | grep C -o || true`    
    if [ ! -z "$RUN" ]; then
        source ../components/azure-common/create-resource-group.sh
        source ../components/azure-storage/create-storage-account.sh
    fi
echo 

echo "***** [I] Setting up INGESTION"
    
    export EVENTHUB_NAMESPACE=$PREFIX"eventhubs"    
    export EVENTHUB_NAME=$PREFIX"in-"$EVENTHUB_PARTITIONS
    export EVENTHUB_CG="cosmos"

    RUN=`echo $STEPS | grep I -o || true`
    if [ ! -z "$RUN" ]; then
        source ../components/azure-event-hubs/create-event-hub.sh
    fi
echo

echo "***** [D] Setting up DATABASE"

    export POSTGRESQL_SERVER_NAME=$PREFIX"timescaledb" 
    export POSTGRESQL_DATABASE_NAME="streaming"
    export POSTGRESQL_TABLE_NAME="rawdata"
    export POSTGRESQL_ADMIN_PASS="Strong_Passw0rd!"  

    RUN=`echo $STEPS | grep D -o || true`
    if [ ! -z "$RUN" ]; then
        source ../components/azure-timescaledb/create-timescaledb.sh
    fi
echo

echo "***** [P] Setting up PROCESSING"

    export PROC_FUNCTION_APP_NAME=$PREFIX"process"
    export PROC_FUNCTION_NAME=StreamingProcessor
    export PROC_PACKAGE_FOLDER=.
    export PROC_PACKAGE_TARGET=CosmosDB    
    export PROC_PACKAGE_NAME=$PROC_FUNCTION_NAME-$PROC_PACKAGE_TARGET.zip
    export PROC_PACKAGE_PATH=$PROC_PACKAGE_FOLDER/$PROC_PACKAGE_NAME

    RUN=`echo $STEPS | grep P -o || true`
    if [ ! -z "$RUN" ]; then
        source ../components/azure-functions/create-processing-function.sh
        source ../components/azure-functions/configure-processing-function-eventhubs.sh
        source ../components/azure-functions/configure-processing-function-cosmosdb.sh
    fi
echo

echo "***** [T] Starting up TEST clients"

    RUN=`echo $STEPS | grep T -o || true`
    if [ ! -z "$RUN" ]; then
        source ../simulator/run-generator-eventhubs.sh
    fi
echo

echo "***** [M] Starting METRICS reporting"

    RUN=`echo $STEPS | grep M -o || true`
    if [ ! -z "$RUN" ]; then
        source ../components/azure-event-hubs/report-throughput.sh
    fi
echo

echo "***** Done"
