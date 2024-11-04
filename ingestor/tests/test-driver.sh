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
    cat ${INPUT} | sed -e 's/"//g' | tr '[:upper:]' '[:lower:]' | sort > ${BASE}.i
    cat ${OUTPUT} | sed -e 's/"//g' | tr '[:upper:]' '[:lower:]' | sort > ${BASE}.o
    diff ${BASE}.i ${BASE}.o
    result=${?}
    return ${result}
}

TEST=${1}
INPUT=${2}
BASE=.$RANDOM
OUTPUT=${BASE}.output.csv

echo "${TEST} - ingesting and dumping ${INPUT}"

run_command ./bin/compile.sh
run_command ../db/db-reset.sh
run_command ./bin/ingest.sh ${INPUT}
run_command ./bin/dump.sh ${OUTPUT}
run_command diff_results
run_command \rm ${BASE}.i ${BASE}.o ${OUTPUT}

echo -e "    \033[0;32mSUCCESS\033[0m\n"

