echo "Count   Payment Type"
tail +2 ${1} | cut -d, -f10 | sort | uniq -c | sort -nr
