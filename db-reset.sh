#!/bin/bash

psql < `dirname ${0}`/db-reset.sql

