#!/bin/sh
step=250
collection="$1"
mkdir "$collection"
for event in successful bid_entered created offer_entered cancelled; 
do
	for i in $(seq 0 $step 10000);
	do
  		export scrape_url=$(printf "https://api.opensea.io/api/v1/events?collection_slug=$collection&only_opensea=false&event_type=$event&offset=$i&limit=$step" "$i");
  		echo $scrape_url;
  		curl --request GET --url $scrape_url > "$collection"/"$event"_$i.json;
                export json_size=$(stat -f "%z" "$collection"/"$event"_$i.json);
                if [[ $(($json_size)) -lt 100 ]]; then
                	break;
                fi;
		sleep 1;
	done
done
