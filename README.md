# Nine

[kubectl-nine](https://github.com/nineinfra/kubectl-nine) is a kubectl plugin to manage the NineInfra and the NineClusters on the k8s.

## Quickstart

1. Install Nine plugin in your k8s cluster
```sh
$ cur -o /usr/local/bin/kubectl-nine -fsSL https://github.com/nineinfra/kubectl-nine/releases/download/v0.4.4/kubectl-nine_0.4.4_linux_amd64
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
postgresql-operator     true            18h
minio-console           true            18h
directpv-controller     true            18h
kyuubi-operator         true            18h
metastore-operator      true            18h
minio-operator          true            18h
nineinfra               true            18h
```

5. Add disks for the NineInfra
```sh
# Probe and save disk information to drives.yaml file.The parameters are planned based on the actual situation of the k8s cluster
$ kubectl nine disk discover --nodes=nos-{13...16} --drives=vd{e...f}

 Discovered node 'nos-13' ✔
 Discovered node 'nos-14' ✔
 Discovered node 'nos-15' ✔
 Discovered node 'nos-16' ✔

┌─────────────────────┬────────┬───────┬──────────┬────────────┬──────┬───────────┬─────────────┐
│ ID                  │ NODE   │ DRIVE │ SIZE     │ FILESYSTEM │ MAKE │ AVAILABLE │ DESCRIPTION │
├─────────────────────┼────────┼───────┼──────────┼────────────┼──────┼───────────┼─────────────┤
│ 253:64$qminruVgo... │ nos-13 │ vde   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:80$Ch6FZ2Ogg... │ nos-13 │ vdf   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:64$E59EefLG5... │ nos-14 │ vde   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:80$ggm4ldlhL... │ nos-14 │ vdf   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:64$AMrZogHry... │ nos-15 │ vde   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:80$YWdsmkPJD... │ nos-15 │ vdf   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:64$IMnKcqGGA... │ nos-16 │ vde   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
│ 253:80$m4f/37aUP... │ nos-16 │ vdf   │ 1000 GiB │ xfs        │ -    │ YES       │ -           │
└─────────────────────┴────────┴───────┴──────────┴────────────┴──────┴───────────┴─────────────┘
Generated 'drives.yaml' successfully.

# Initialize selected disks.
$ kubectl nine disk init drives.yaml --dangerous

 Processed initialization request 'ab5160e6-3b1d-458b-9c79-b72a9fde8694' for node 'nos-14' ✔
 Processed initialization request '06e15423-67a6-47ae-bca3-94b6d33959f1' for node 'nos-15' ✔
 Processed initialization request '8d313b38-1ac0-4891-82a8-999087815962' for node 'nos-16' ✔
 Processed initialization request '326d2668-af14-4d1c-bdb9-676a8e833db6' for node 'nos-13' ✔


┌──────────────────────────────────────┬────────┬───────┬─────────┐
│ REQUEST_ID                           │ NODE   │ DRIVE │ MESSAGE │
├──────────────────────────────────────┼────────┼───────┼─────────┤
│ 326d2668-af14-4d1c-bdb9-676a8e833db6 │ nos-13 │ vde   │ Success │
│ 326d2668-af14-4d1c-bdb9-676a8e833db6 │ nos-13 │ vdf   │ Success │
│ ab5160e6-3b1d-458b-9c79-b72a9fde8694 │ nos-14 │ vde   │ Success │
│ ab5160e6-3b1d-458b-9c79-b72a9fde8694 │ nos-14 │ vdf   │ Success │
│ 06e15423-67a6-47ae-bca3-94b6d33959f1 │ nos-15 │ vde   │ Success │
│ 06e15423-67a6-47ae-bca3-94b6d33959f1 │ nos-15 │ vdf   │ Success │
│ 8d313b38-1ac0-4891-82a8-999087815962 │ nos-16 │ vde   │ Success │
│ 8d313b38-1ac0-4891-82a8-999087815962 │ nos-16 │ vdf   │ Success │
└──────────────────────────────────────┴────────┴───────┴─────────┘
```

6. Create a NineCluster
```sh
# The datavolume arg expresses the storage capacity size of the NineCluster,default unit is Gi.
$ kubectl create namespace dwh
$ kubectl nine create nine-test -v 16 -n dwh
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
nine-test-nine-metastore                        metastore       statefulset     1/1             18h
nine-test-nine-ss-0                             minio           statefulset     4/4             18h
nine-test-nine-pg                               postgresql      cluster         3/3             18h
nine-test-nine-kyuubi                           kyuubi          statefulset     1/1             18h
```
9. Execute sql statements in the NineCluster
```sh
# The first to do the sql in the NineCluster may take tens of seconds because it is in a cold start state.
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

## Contributing
Contributing is very welcome.

## License

Copyright 2023 nineinfra.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
