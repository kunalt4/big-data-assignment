#!/bin/bash
total_columns=$(sed -n 2p crimedata-australia.csv | tr ',' '\n' | wc -l)
total=$(tail +2 crimedata-australia.csv | wc -l)
sum=0; 
sum=0; while read num; do ((sum += num)); done < <(cut -f3 -d, crimedata-australia.csv | tail +1); 
min=$(echo "$sum / $total" | bc -l )

min_city=$(head -1 crimedata-australia.csv | cut -f1 -d,)

for (( i=4; i<=$total_columns; i++ ))
do
	sum=0; 
	while read num; 
	do 
		((sum += num)); 
	done < <(cut -f"$i" -d, crimedata-australia.csv | tail +1);
	average=$(echo "scale=3; $sum / $total" | bc -l )
	if (( $(echo "$average < $min" | bc -l) )); 
	then
		min=$average
		min_city=$(head -1 crimedata-australia.csv | cut -f"$i" -d,)
	fi
done 
echo "Minimum crime is in $min_city"
echo "Minimum average crime rate: $min"
