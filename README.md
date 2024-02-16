# Nine

[kubectl-nine](https://github.com/nineinfra/kubectl-nine) is a kubectl plugin to manage the NineInfra and the NineClusters on the k8s.

## Quickstart

1. Install Nine plugin in your k8s cluster
```sh
$ curl -o /usr/local/bin/kubectl-nine -fsSL https://github.com/nineinfra/kubectl-nine/releases/download/v0.7.0/kubectl-nine_0.7.0_linux_amd64
$ chmod 0755 /usr/local/bin/kubectl-nine
```

2. Prepare env for the Nine
```sh
$ kubectl nine prepare
```

3. Install the NineInfra
```sh
$ kubectl nine install
```

4. Show the status of the NineInfra
```sh
# If the READYs are all true and then the NineInfra is at work
$ kubectl nine status

NAME                    READY           AGE
postgresql-operator     true            2m39s
minio-console           true            3m5s
directpv-controller     true            3m9s
doris-operator          true            2m55s
hdfs-operator           true            3m11s
kyuubi-operator         true            2m26s
metastore-operator      true            2m18s
minio-operator          true            3m5s
nineinfra               true            2m47s
zookeeper-operator      true            3m17s

```

5. Create a storage pool for the NineInfra
```sh
# Probe and save disk information to drives.yaml file.The parameters are planned based on the actual situation of the k8s cluster
$ kubectl nine storage -c create --storage-pool nineinfra-default --dangerous --nodes=nos-{13...16} --drives=sd{c...f}
 
 Discovered node 'nos-13' ✔
 Discovered node 'nos-14' ✔
 Discovered node 'nos-15' ✔
 Discovered node 'nos-16' ✔

┌─────────────────────┬───────┬───────┬─────────┬────────────┬─────────────┬───────────┬─────────────┐
│ ID                  │ NODE  │ DRIVE │ SIZE    │ FILESYSTEM │ MAKE        │ AVAILABLE │ DESCRIPTION │
├─────────────────────┼───────┼───────┼─────────┼────────────┼─────────────┼───────────┼─────────────┤
│ 8:32$C9H9nKceHdl... │ nos-13│ sdc   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:48$PEwqlggXwL/... │ nos-13│ sdd   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:64$np7aYFFxYiO... │ nos-13│ sde   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:80$e6Nukqaqczo... │ nos-13│ sdf   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:32$PUW82s10No9... │ nos-14│ sdc   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:48$XATZhOxft8i... │ nos-14│ sdd   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:64$RA2WSliDuk2... │ nos-14│ sde   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:80$rnRd/XXHQb9... │ nos-14│ sdf   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:32$V2Koy98ZHuH... │ nos-15│ sdc   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:48$VSkTOOfGpnP... │ nos-15│ sdd   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:64$MwJeUa9LQDJ... │ nos-15│ sde   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:80$rtiZxtNcIbc... │ nos-15│ sdf   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:32$cT0j4bEhHSl... │ nos-16│ sdc   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:48$NhtoJpWwr4X... │ nos-16│ sdd   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:64$zjL8PF4HICC... │ nos-16│ sde   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
│ 8:80$NoQZ3iBqKb8... │ nos-16│ sdf   │ 837 GiB │ xfs        │ SMC LSI2208 │ YES       │ -           │
└─────────────────────┴───────┴───────┴─────────┴────────────┴─────────────┴───────────┴─────────────┘

Generated 'drives.yaml' successfully.

 ███████████████████████████████████ 100%

 Processed initialization request '096c865d-ed2e-47ec-85f0-ca7999299157' for node 'nos-13' ✔
 Processed initialization request '25237a70-e8d9-4f88-8eea-298014184bc0' for node 'nos-14' ✔
 Processed initialization request 'adef5a4f-0bb5-4999-8652-9f3fb5b35616' for node 'nos-15' ✔
 Processed initialization request 'a15ef873-4373-4990-9ed0-7d1411845424' for node 'nos-16' ✔

┌──────────────────────────────────────┬───────┬───────┬─────────┐
│ REQUEST_ID                           │ NODE  │ DRIVE │ MESSAGE │
├──────────────────────────────────────┼───────┼───────┼─────────┤
│ a15ef873-4373-4990-9ed0-7d1411845424 │ nos-13│ sdc   │ Success │
│ a15ef873-4373-4990-9ed0-7d1411845424 │ nos-13│ sdd   │ Success │
│ a15ef873-4373-4990-9ed0-7d1411845424 │ nos-13│ sde   │ Success │
│ a15ef873-4373-4990-9ed0-7d1411845424 │ nos-13│ sdf   │ Success │
│ 096c865d-ed2e-47ec-85f0-ca7999299157 │ nos-14│ sdc   │ Success │
│ 096c865d-ed2e-47ec-85f0-ca7999299157 │ nos-14│ sdd   │ Success │
│ 096c865d-ed2e-47ec-85f0-ca7999299157 │ nos-14│ sde   │ Success │
│ 096c865d-ed2e-47ec-85f0-ca7999299157 │ nos-14│ sdf   │ Success │
│ 25237a70-e8d9-4f88-8eea-298014184bc0 │ nos-15│ sdc   │ Success │
│ 25237a70-e8d9-4f88-8eea-298014184bc0 │ nos-15│ sdd   │ Success │
│ 25237a70-e8d9-4f88-8eea-298014184bc0 │ nos-15│ sde   │ Success │
│ 25237a70-e8d9-4f88-8eea-298014184bc0 │ nos-15│ sdf   │ Success │
│ adef5a4f-0bb5-4999-8652-9f3fb5b35616 │ nos-16│ sdc   │ Success │
│ adef5a4f-0bb5-4999-8652-9f3fb5b35616 │ nos-16│ sdd   │ Success │
│ adef5a4f-0bb5-4999-8652-9f3fb5b35616 │ nos-16│ sde   │ Success │
│ adef5a4f-0bb5-4999-8652-9f3fb5b35616 │ nos-16│ sdf   │ Success │
└──────────────────────────────────────┴───────┴───────┴─────────┘
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-16/sde
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-14/sdf
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-15/sdd
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-14/sdd
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-14/sdc
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-13/sde
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-16/sdf
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-16/sdc
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-13/sdc
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-15/sde
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-13/sdf
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-15/sdc
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-16/sdd
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-14/sde
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-13/sdd
Label 'directpv.min.io/storage-pool:nineinfra-default' successfully set on nos-15/sdf
```

6. Create a NineCluster
```sh
# The datavolume arg expresses the storage capacity size of the NineCluster,default unit is Gi.
$ kubectl create namespace dwh
# Specify the hdfs as the main storage for the NineInfra Cluster,default is minio.
# You can add the doris as the default olap for the NineInfra cluster by the parameter --olap
$ kubectl nine create nine-test -n dwh -v 16 --enable-kyuubi-ha --olap doris --main-storage hdfs
```

7. List the NineClusters
```sh
# The READY flag indicates whether the NineCluster is working properly.
$ kubectl nine list
NAME                    DATAVOLUME      READY           NAMESPACE       AGE
nine-test               16              true            dwh             18h
```
8. Show the NineCluster
```sh
# This will show all the projects status in the NineCluster.
$ kubectl nine show nine-test -n dwh
NAME                                            PROJECT         TYPE            READY           AGE
nine-test-nine-kyuubi                           kyuubi          statefulset     2/2             119m
nine-test-nine-metastore                        metastore       statefulset     1/1             119m
nine-test-nine-pg                               postgresql      cluster         3/3             146m
nine-test-nine-doris-be                         doris-be        statefulset     3/3             126m
nine-test-nine-doris-fe                         doris-fe        statefulset     3/3             146m
nine-test-nine-hdfs-datanode                    datanode        statefulset     4/4             121m
nine-test-nine-hdfs-journalnode                 journalnode     statefulset     3/3             125m
nine-test-nine-hdfs-namenode                    namenode        statefulset     2/2             122m
nine-test-nine-zookeeper                        zookeeper       statefulset     3/3             146m

```
9. Execute sql statements in the NineCluster
```sh
# The first to do the sql in the NineCluster may take tens of seconds because it is in a cold start state.
# Notice: Currently, you can only execute SQL commands within the k8s cluster.
$ kubectl nine sql nine-test -n dwh -s "show databases"
+-----------+
| NAMESPACE |
+-----------+
| default   |
+-----------+
# Create a database
$ kubectl nine sql nine-test -n dwh -s "create database test"
$ kubectl nine sql nine-test -n dwh -s "show databases"
+-----------+
| NAMESPACE |
+-----------+
| default   |
| test      |
+-----------+
# Create a table
$ kubectl nine sql nine-test -n dwh -s "create table test.test(id int,name string)"
$ kubectl nine sql nine-test -n dwh -s "show tables from test"
+-----------+-------------+-----------+
| TABLENAME | ISTEMPORARY | NAMESPACE |
+-----------+-------------+-----------+
| test      | false       | test      |
+-----------+-------------+-----------+
# Insert into values
$ kubectl nine sql nine-test -n dwh -s "insert into test.test values(1,'nineinfra'),(2,'kyuubi'),(3,'spark'),(4,'minio')"
# Query a table
$ kubectl nine sql nine-test -n dwh -s "select * from test.test"
+----+-----------+
| ID | NAME      |
+----+-----------+
| 1  | nineinfra |
| 2  | kyuubi    |
| 3  | spark     |
| 4  | minio     |
+----+-----------+
```
10. Interactive sql in the NineCluster
```sh
# kubectl nine sql nine-test --tty -ndwh
0: jdbc:hive2://10.99.243.86:10009> show databases;
show databases;
+---------------+
|   namespace   |
+---------------+
| default       |
| test          |
| tpcds_nine01  |
+---------------+
0: jdbc:hive2://10.99.243.86:10009> show tables from test;
show tables from test;
+------------+------------+--------------+
| namespace  | tableName  | isTemporary  |
+------------+------------+--------------+
| test       | test       | false        |
+------------+------------+--------------+
0: jdbc:hive2://10.99.243.86:10009> select * from test.test;
select * from test.test;
+-----+------------+
| id  |    name    |
+-----+------------+
| 1   | hadoop     |
| 1   | nineinfra  |
+-----+------------+
0: jdbc:hive2://10.99.243.86:10009> insert into test.test values(2,"spark");
insert into test.test values(2,"spark");
+---------+
| Result  |
+---------+
+---------+
0: jdbc:hive2://10.99.243.86:10009> select * from test.test;
select * from test.test;
+-----+------------+
| id  |    name    |
+-----+------------+
| 2   | spark      |
| 1   | hadoop     |
| 1   | nineinfra  |
+-----+------------+
0: jdbc:hive2://10.99.243.86:10009>
```
## Using tools for data analysis on the data warehouse
1. Install the tools
```sh
# This will show all the projects status in the NineCluster.
$ kubectl nine tools install -t "airflow,superset,nifi" -n dwh --airflowrepo 192.168.123.24:30003/library/airflow
# The Airflow,Superset and Nifi will be installed. And the dependencies will be installed automatically.
# The hive database connection of the NineCluster in the namespace dwh will be added to the Superset automatically.
```
2. List the tools
```sh
$ kubectl nine tools list
NINENAME                TOOLNAME        READY           NAMESPACE       ACCESS
nine-test               zookeeper       1/1             dwh             10.106.31.92:2181
nine-test               airflow         4/4             dwh             http://172.18.123.24:30406
nine-test               superset        4/4             dwh             http://172.18.123.24:32326
nine-test               nifi            1/1             dwh             https://172.18.123.24:31333
nine-test               redis           1/1             dwh             redis://10.102.223.53:6379
# The READY column indicates the status of the tool, where true means the tool is ready and false means the tool is 
# still being prepared.
# The ACCESS column displays the access method for the tool, which you can directly copy and paste. For example, 
# you can copy the access method https://172.18.123.24:31333 for the NiFi tool and paste it into the address bar 
# of your browser to open it directly.
```
3. Uninstall the tools
```sh
# kubectl nine tools uninstall -t "airflow,superset,nifi,redis,zookeeper" -n dwh
Uninstall nineinfra-airflow successfully!
Uninstall nineinfra-superset successfully!
Uninstall nineinfra-nifi successfully!
Uninstall nineinfra-redis successfully!
Uninstall nineinfra-zookeeper successfully!
```
## Running tpcds on the NineCluster
1. Generate data
```sh
$ kubectl nine tpcds nine-test -g -d tpcds_nine01 -n dwh
```
2. Run benchmark
```sh
$ kubectl nine tpcds nine-test -d tpcds_nine01 -n dwh
# By default,the results will be stored in s3a://nineinfra/datahouse/performance
```

## Contributing
Contributing is very welcome.

## License

Copyright 2024 nineinfra.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
