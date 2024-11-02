#!/bin/bash

export INGESTOR_DB_CSV_INPUT="../../data/raw_product_data.100.csv"

if [ ${#} -gt 0 ]; then
    export INGESTOR_DB_CSV_INPUT=${1}
    shift
else
    echo "Missing csv file path, using ${CSVFILE} as default"
fi

export INGESTOR_DB_URL="jdbc:postgresql://localhost:5432/inventory"
export INGESTOR_DB_USER="nico"
export INGESTOR_DB_PASSWORD=""
export INGESTOR_DB_AUTOCOMMIT=true

mvn exec:java -e -Dexec.mainClass="ca.ulex.Ingestor" -Dexec.args="${*}"

