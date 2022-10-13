#!/usr/bin/env bash
~/hadoop-3.3.4/sbin/stop-dfs.sh
rm -rf ~/hadoop-datanode/*
rm -rf ~/hadoop-namenode/*
~/hadoop-3.3.4/bin/hdfs namenode -format
~/hadoop-3.3.4/sbin/start-dfs.sh
~/hadoop-3.3.4/bin/hdfs dfs -mkdir /user
~/hadoop-3.3.4/bin/hdfs dfs -mkdir /user/shkr
~/hadoop-3.3.4/bin/hdfs dfs -mkdir /user/shkr/pi-calculation
~/hadoop-3.3.4/bin/hadoop jar ~/pi-calculation/pi.jar Pi /user/shkr/pi-calculation




