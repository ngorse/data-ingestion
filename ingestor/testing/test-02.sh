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
    cat ${INPUT} | cut -d, -f1,2,5,7,8,9 > ${BASE}.i
    cat ${OUTPUT} | cut -d, -f1,2,5,7,8,9 > ${BASE}.o
    diff ${BASE}.i ${BASE}.o
    result=${?}
    \rm ${BASE}.i ${BASE}.o ${OUTPUT}
    return ${result}
}

BASE=.$RANDOM
DATA=`dirname ${0}`/data
INPUT=${DATA}/input.amiparis+adamo.csv
OUTPUT=${DATA}/${BASE}.output.amiparis+adamo.csv

echo "test-01 - ingesting and dumping ${INPUT}"

run_command ./compile.sh
run_command ../db-reset.sh
run_command ./ingest.sh ${INPUT}
run_command ./dump.sh ${OUTPUT}
run_command diff_results

echo -e "\033[0;32mSUCCESS\033[0m"

