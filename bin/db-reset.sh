#!/bin/bash

psql -h localhost -p 5432 -U ingestor -d inventory < `dirname ${0}`/initdb/db_reset.sql

