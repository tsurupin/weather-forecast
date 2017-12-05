#!/bin/bash

for f in ./db_schema/*; do
  case "$f" in
    *.cql) cqlsh -f "$f" $1 $2 --cqlversion=$3;
  esac
done