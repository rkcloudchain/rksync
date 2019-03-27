#!/usr/bin/env bash
#
# Copyright Rockontrol Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -e
echo "" > coverage.txt

for d in $(go list ./... | grep -v vendor); do
    go test -race -coverprofile=profile.out -covermode=atomic "$d"
    if [ -f profile.out ]; then
        cat profile.out >> coverage.txt
        rm profile.out
    fi
done