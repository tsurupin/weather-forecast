#!/bin/bash
# Copyright (c) Jupyter Development Team.
# Distributed under the terms of the Modified BSD License.

set -e
echo "starting"
exec jupyter notebook > /dev/null 2>&1
