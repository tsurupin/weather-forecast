#!/bin/bash

set -ex

echo "execute migration files"


function run_migrations() {
  files="/usr/local/migrations/* "
  for f in $files; do
    case $f in
      *.cql) cqlsh -f $f
    esac
  done
}

function create_seed_data() {
  files="/usr/local/seeds/* "
  for f in $files; do
    case $f in
      *.cql) cqlsh -f $f
    esac
  done
}

function print_error() {
  echo "some error happens"
  exit 1
}

run_migrations || print_error
echo "migration is done!"
create_seed_data || print_error
echo "seed_data creation is done"
