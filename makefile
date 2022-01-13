.PHONY: run_sql_example
run_sql_example:
	bq query --use_legacy_sql=false --format=csv `ggrep -v '\-\-' stat.sql` > results_example.csv

.PHONY: run_sql_all
# need a loog time to download
run_sql_all:
	bq query --use_legacy_sql=false --format=csv --max_rows=200000 "`ggrep -v '\-\-' stat.sql`" > results.csv