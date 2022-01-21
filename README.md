## 统计 2021 年国服玩家游玩信息
[![Backend Go](https://github.com/QingGo/ddnet_2022_chn_stat_backend/actions/workflows/go.yml/badge.svg)](https://github.com/QingGo/ddnet_2022_chn_stat_backend/actions/workflows/go.yml)
[![Frontend Node.js CI](https://github.com/QingGo/ddnet_2022_chn_stat_frontend/actions/workflows/node.js.yml/badge.svg)](https://github.com/QingGo/ddnet_2022_chn_stat_frontend/actions/workflows/node.js.yml)

目前已经完成 [1.0.0](https://github.com/QingGo/ddnet_2022_chn_stat/milestone/1) 版本。可以在 http://49.232.3.102:22223/ 进行体验。

* 后端：https://github.com/QingGo/ddnet_2022_chn_stat_backend
* 前端：https://github.com/QingGo/ddnet_2022_chn_stat_frontend
* bigquery 使用说明：how_to_query_ddnet_stat_in_bigquery.md

从 1 月 12 日测试版上线起，截止至 1 月 21 日，访问量已经逐渐接近 0。现总 PV 为 2850，总 UV 为 754。以下是相关统计命令：
``` shell
~/ddnet_2022_chn_stat_deploy$ cat ddnet_2022_chn_stat_backend_linux64.log | grep "| GET" | awk '{print $10}' |  wc -l
2850
~/ddnet_2022_chn_stat_deploy$ cat ddnet_2022_chn_stat_backend_linux64.log | grep "| GET" | awk '{print $10}' | sort | uniq | wc -l
754
```

