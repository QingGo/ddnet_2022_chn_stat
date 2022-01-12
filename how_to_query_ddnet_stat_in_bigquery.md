execute these commands in you gcp cloub shell
``` bash
wget https://ddnet.tw/stats/ddnet-stats.zip
unzip ddnet-stats.zip

# load data into bigquery
bq load --autodetect --source_format=CSV --replace ddnet_stats.maps ./ddnet-stats/maps.csv
# skip \\" and only replact \" to ""
sed '/\\\\"/ ! s/\\"/""/g' ./ddnet-stats/race.csv > ./ddnet-stats/race_clean.csv
bq load --autodetect --source_format=CSV --replace --max_bad_records=1000 --allow_quoted_newlines ddnet_stats.race ./ddnet-stats/race_clean.csv
sed '/\\\\"/ ! s/\\"/""/g' ./ddnet-stats/teamrace.csv > ./ddnet-stats/teamrace_clean.csv
bq load --autodetect --source_format=CSV --replace --max_bad_records=1000 --allow_quoted_newlines ddnet_stats.teamrace ./ddnet-stats/teamrace_clean.csv
# don't know why there are still mull Error like
# b'Error while reading data, error message: Error detected while parsing row starting at position: 51610227. Error: Data between close double quote (") and field separator.'
# just ignore
```
Then happy writing SQL.