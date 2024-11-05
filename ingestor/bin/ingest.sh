#!/bin/bash

if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi


if [ ${#} -gt 0 ]; then
    export INGESTOR_DB_CSV_INPUT=${1}
    echo -n "Using given CSV input file: "
    shift
elif [ ! -z ${INGESTOR_DB_CSV_DEFAULT_INPUT} ]; then
    export INGESTOR_DB_CSV_INPUT=${INGESTOR_DB_CSV_DEFAULT_INPUT}
    echo -n "Using default CSV input file: "
else
    echo "Usage: `basename ${0}` <input csv file>"
    exit 1
fi

echo ${INGESTOR_DB_CSV_INPUT}
mvn exec:java -e -Dexec.mainClass="ca.ulex.Ingestor" -Dexec.args="${*}"
