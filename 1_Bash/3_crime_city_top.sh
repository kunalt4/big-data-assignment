#!/bin/bash
sort -k"$1" -t, -nr crimedata-australia.csv | cut -f1 -d, | head -1
