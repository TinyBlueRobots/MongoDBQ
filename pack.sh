#!/bin/bash
NUGETVERSION=1.2.0
dotnet pack src/MongoDBQ.csproj -c Release /p:PackageVersion=$NUGETVERSION
dotnet nuget push src/bin/Release/MongoDBQ.$NUGETVERSION.nupkg -k "$NUGETKEY" -s nuget.org
