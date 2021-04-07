#!/bin/sh
step=250
event="created"
for i in $(seq 0 $step 10000);
do
  export scrape_url=$(printf "https://api.opensea.io/api/v1/events?collection_slug=cryptopunks&only_opensea=false&event_type=$event&offset=$i&limit=$step" "$i");
  echo $scrape_url;
  curl --request GET --url $scrape_url > "$event"_$i.json;
done
