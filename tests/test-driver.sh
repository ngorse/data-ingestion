#!/bin/bash

function run_command()
{
    echo -n "    ${*}"
    ${*} >& /dev/null
    if [ $? -ne 0 ]; then
        echo -e " - \033[0;31mfailed\033[0m"
        echo -e "    \033[0;31mFAILURE\033[0m\n"
        exit 1
    else
        echo -e " - \033[0;32mpassed\033[0m"
    fi
}

function dump_sql()
{
    DB_NAME=inventory
    HOST=localhost
    USER=ingestor
    tables=$(psql -h localhost -p 5432 -U "${USER}" -d "${DB_NAME}" -t -c "SELECT table_name FROM information_schema.tables WHERE table_schema='public';")

    for table in $tables; do
      if [[ -n "$table" ]]; then
        echo "Table: $table"
        psql -h localhost -U "${USER}" -d "${DB_NAME}" -c "\copy ${table} TO STDOUT CSV HEADER"
        echo ""
      fi
    done > ${OUTPUT}.DB
}


TEST=${1}
INPUT=${2}
OUTPUT=.TMP.${TEST}.csv.DUMP

echo "${TEST} - ingesting and dumping ${INPUT}"

run_command ./bin/compile.sh
run_command ./bin/db-reset.sh
run_command ./bin/ingest.sh ${INPUT}

run_command ./bin/dump.sh ${OUTPUT}.raw
sort -r ${OUTPUT}.raw > ${OUTPUT}
run_command diff ${INPUT}.REF-DUMP ${OUTPUT}

run_command dump_sql
run_command diff ${INPUT}.REF-DB ${OUTPUT}.DB

run_command \rm ${OUTPUT}.raw ${OUTPUT} ${OUTPUT}.DB

echo -e "    \033[0;32mSUCCESS\033[0m\n"

