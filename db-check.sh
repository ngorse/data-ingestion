#!/bin/bash

for t in product variant metadata; do
    echo -e "\n${t}\n"
    echo "select * from ${t}" | psql -d inventory
done

