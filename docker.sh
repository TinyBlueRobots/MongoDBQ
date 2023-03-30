#!/bin/bash
if docker ps -a | grep mongo >/dev/null; then
  docker start mongo
else
  docker run --name mongo -p 27017:27017 -d mongo
fi

