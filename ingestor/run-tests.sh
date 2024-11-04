#!/bin/bash

ls ./testing/test-[0-9]* | while read t; do
    ${t}
done

