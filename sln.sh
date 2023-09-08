#!/bin/bash
rm *.sln
dotnet new sln
find -name "*proj" | xargs -I {} dotnet sln add {}
