#!/bin/bash

FILE=`dirname ${0}`/db-reset.sql
TABLES=`grep CREATE\ TABLE ${FILE} | grep -v -- -- | cut -d\  -f3`

for table in ${TABLES}; do
    echo -e "\n${table}\n"
    echo "select * from ${table}" | psql -d inventory
done

