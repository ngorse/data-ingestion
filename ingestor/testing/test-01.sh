#!/bin/bash

function run_command()
{
    echo -n "    ${*}"
    ${*} >& /dev/null
    if [ $? -ne 0 ]; then
        echo -e " - \033[0;31mfailed\033[0m"
        echo -e "\033[0;31mFAILURE\033[0m"
        exit 1
    else
        echo -e " - \033[0;32mpassed\033[0m"
    fi
}

function diff_results()
{
    BASE=.$RANDOM
    cat ${INPUT} | cut -d, -f2,5 > ${BASE}.x
    cat ${OUTPUT} | cut -d, -f2,5 > ${BASE}.y
    diff ${BASE}.x ${BASE}.y
    result=${?}
    \rm ${BASE}.x ${BASE}.y
    return ${result}
}

DATA=`dirname ${0}`/data
INPUT=${DATA}/input.amiparis+adamo.csv
OUTPUT=${DATA}/output.amiparis+adamo.csv

echo "test-01 - ingesting and dumping ${INPUT}"

run_command ./compile.sh
run_command ../db-reset.sh
run_command ./ingest.sh ${INPUT}
run_command ./dump.sh ${OUTPUT}
run_command diff_results

echo -e "\033[0;32mSUCCESS\033[0m"

