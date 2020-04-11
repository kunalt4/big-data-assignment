#!/bin/bash
total=$(tail +2 crimedata-australia.csv | wc -l)
sum=0; while read num; do ((sum += num)); done < <(cut -f"$1" -d, crimedata-australia.csv | tail +1); echo "scale=3; $sum / $total"| bc -l
