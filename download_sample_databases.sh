#!/bin/sh

echo "Downloading superheroes.db: ~1MB (used in stage 7)"
curl -Lo superheroes.struct https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/superheroes.db

echo "Downloading companies.db: ~7MB (used in stage 8)"
curl -Lo companies.struct https://raw.githubusercontent.com/codecrafters-io/sample-sqlite-databases/master/companies.db

echo "Sample databases downloaded."
