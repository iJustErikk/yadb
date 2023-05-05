#!/bin/bash
docker build -t yadb .
docker run --rm -it -v $(pwd)/data:/usr/src/app/yadb yadb