#!/usr/bin/env bash
# Import collections into mongodb
# Two arguments required are
# 1. database name
# 2. csv file names separated by space

# example usage
# ./mongo_import.sh dev Players.csv Teams.csv

if [ $# -lt 2 ]; then
	echo "Two or more arguments needed:"
	echo "1. database name"
	echo "2. csv file names separated by space"
	exit 1
fi


for file in "${@:2}"; do
	file_name=${file}
	collection_name=$(echo $file_name | cut -f2 -d "/" | cut -f1 -d ".")
	mongoimport -d $1 -c $collection_name --type CSV --file $file_name --headerline
	echo "Collection $collection_name created."
done
