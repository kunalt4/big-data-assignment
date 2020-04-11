#!/bin/bash
sum=0;
while read num;
do ((sum += num));
done < <(cut -f"$1" -d"," crimedata-australia.csv | tail +1); echo $sum
