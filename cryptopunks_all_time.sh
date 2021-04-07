#!/bin/sh

step=250
seconds_24h=2628000
collection="$1"
mkdir "$collection"

for datevar in $(seq -f %1.0f 1483228800 $seconds_24h 1617926400);
do 
	export next=$(($datevar + $seconds_24h))
	for event in transfer successful created cancelled; 
	do
		for i in $(seq 0 $step 10000);
		do
  			export scrape_url=$(printf "https://api.opensea.io/api/v1/events?collection_slug=$collection&occurred_before=$next&occurred_after=$datevar&event_type=$event&offset=$i&limit=$step" "$i");
  			echo "$scrape_url";
  			curl --request GET --url $scrape_url > "$collection"/$(date -r $next "+%Y-%m-%d")_"$event"_$i.json;
			export json_size=$(stat -f "%z" "$collection"/$(date -r $next "+%Y-%m-%d")_"$event"_$i.json);
			if [[ $(($json_size)) -lt 100 ]]; then
  			sleep 1;
				break;
			fi;
			sleep 1;
		done
	done
done
