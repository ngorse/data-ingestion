#!/bin/bash

ls ./tests/test-[0-9]* | while read t; do
    ${t}
done

