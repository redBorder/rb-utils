#!/bin/bash

java -jar gridgain-stast-1.0.jar $1 $2 2>&1| grep -v '[0-9][0-9]:[0-9][0-9]:[0-9][0-9]'
