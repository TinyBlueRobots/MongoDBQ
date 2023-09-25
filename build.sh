#!/bin/bash
set -e
trap 'docker-compose down' EXIT
docker-compose up -d
dotnet tool restore
dotnet paket install
dotnet run --project ./tests/MongoDBQ.Tests.fsproj
docker-compose down
