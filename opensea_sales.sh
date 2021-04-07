#!/bin/sh
step=250
collection="$1"
mkdir "$collection"
for event in successful; 
do
	for i in $(seq 0 $step 10000);
	do
  		export scrape_url=$(printf "https://api.opensea.io/api/v1/events?&event_type=$event&offset=$i&limit=$step" "$i");
  		echo $scrape_url;
  		curl --request GET --url $scrape_url > "$collection"/"$event"_$i.json;
		sleep 1;
	done
done
