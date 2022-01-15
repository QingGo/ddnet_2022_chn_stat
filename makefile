user = zeng
server_ip = 49.232.3.102
ssh_server = $(user)@$(server_ip)

.PHONY: test_sql
test_sql:
	bq query --use_legacy_sql=false `ggrep -v '\-\-' stat.sql`

.PHONY: run_sql_example
run_sql_example:
	bq query --use_legacy_sql=false --format=csv `ggrep -v '\-\-' stat.sql` > results_example.csv

.PHONY: run_sql_all
# need a loog time to download
run_sql_all:
	bq query --use_legacy_sql=false --format=csv --max_rows=200000 "`ggrep -v '\-\-' stat.sql`" > results.csv

.PHONY: deploy_db
deploy_db: run_sql_all
	scp ./results.csv $(ssh_server):~/ddnet_2022_chn_stat_deploy

.PHONY: deploy_be 
deploy_be:
	cd ddnet_2022_chn_stat_backend; \
	GOOS=linux GOARCH=amd64 go build -o ddnet_2022_chn_stat_backend_linux64 main.go; \
	scp -r ./ddnet_2022_chn_stat_backend_linux64 $(ssh_server):~/ddnet_2022_chn_stat_deploy/ddnet_2022_chn_stat_backend_linux64_new
	ssh $(ssh_server) "ps -aux | grep ddnet_2022_chn_stat_backend_linux64 | grep -v 'grep' | awk '{print \$$2}' | xargs kill || true"
	ssh $(ssh_server) "cd ddnet_2022_chn_stat_deploy; \
		mv ddnet_2022_chn_stat_backend_linux64_new ddnet_2022_chn_stat_backend_linux64 || true"
	ssh $(ssh_server) "cd ddnet_2022_chn_stat_deploy; \
		CSV_FILE_PATH=./results.csv nohup ./ddnet_2022_chn_stat_backend_linux64 >> ddnet_2022_chn_stat_backend_linux64.log 2>&1 &"
	
.PHONY: deploy_fe
deploy_fe:
	cd ddnet_2022_chn_stat_frontend; \
	npm run build; \
	npm run upload;