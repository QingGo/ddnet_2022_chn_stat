execute these commands in you gcp cloub shell
``` bash
wget https://ddnet.tw/stats/ddnet-stats.zip
unzip ddnet-stats.zip

# create a bucket, use you own unique name
gsutil mb gs://you_own_bucket_name
gsutil ls
# copy data files to bucket
gsutil cp ./ddnet-stats/* gs://you_own_bucket_name/


# load data into bigquery
bq load --autodetect --source_format=CSV --replace ddnet_stats.maps gs://you_own_bucket_name/maps.csv
bq load --autodetect --source_format=CSV --replace --max_bad_records=100000 ddnet_stats.race gs://you_own_bucket_name/race.csv
bq load --autodetect --source_format=CSV --replace --max_bad_records=100000 ddnet_stats.teamrace gs://you_own_bucket_name/teamrace.csv
```
Then happy writing SQL.