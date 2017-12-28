#!/bin/bash

echo "execute migration files"
for f in ./migrations/*; do
  case $f in
    *.cql) cqlsh -f $f
  esac
done

echo "execute seed files"
for f in ./seeds/*; do
  echo $f
done
