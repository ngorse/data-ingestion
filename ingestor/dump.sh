#!/bin/bash

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi


if [ ${#} -gt 0 ]; then
    export INGESTOR_DB_CSV_OUTPUT=${1}
    echo -n "Using given CSV OUTPUT file: "
    shift
elif [ ! -z ${INGESTOR_DB_CSV_DEFAULT_OUTPUT} ]; then
    export INGESTOR_DB_CSV_OUTPUT=${INGESTOR_DB_CSV_DEFAULT_OUTPUT}
    echo -n "Using default CSV OUTPUT file: "
else
    echo "Usage: `basename ${0}` <OUTPUT csv file>"
    exit 1
fi

echo ${INGESTOR_DB_CSV_OUTPUT}
mvn exec:java -e -Dexec.mainClass="ca.ulex.DumpCSV" -Dexec.args="${*}"

