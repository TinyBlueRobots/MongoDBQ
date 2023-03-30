#!/bin/bash
set -e
./docker.sh
dotnet tool restore
dotnet paket install
dotnet run --project ./tests/MongoDBQ.Tests.fsproj
