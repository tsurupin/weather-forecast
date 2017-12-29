#!/bin/bash

set -ex

echo "execute migration files"


function run_migrations() {
  files="/usr/local/* "
  for f in $files; do
    case $f in
      *.cql) cqlsh -f $f
    esac
  done
  exit 1
}

function print_error() {
  echo "some error happens"
  exit 1
}

run_migrations || print_error
