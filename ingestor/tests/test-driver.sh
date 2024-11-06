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

TEST=${1}
INPUT=${2}
OUTPUT=.TMP.${TEST}.csv.DUMP

echo "${TEST} - ingesting and dumping ${INPUT}"

run_command ./bin/compile.sh
run_command ../db/db-reset.sh
run_command ./bin/ingest.sh ${INPUT}
run_command ./bin/dump.sh ${OUTPUT}.raw
sort -r ${OUTPUT}.raw > ${OUTPUT}
run_command diff ${INPUT}.REF-DUMP ${OUTPUT}
run_command \rm ${OUTPUT}.raw ${OUTPUT}

echo -e "    \033[0;32mSUCCESS\033[0m\n"

