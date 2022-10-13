# Pi Computation Using Hadoop
I will not explain how to [install Hadoop and Java](cs570_week2_hw1_shahadat_19609.pdf) into local system. I am assuming that Hadoop and Java already installed and working properly.

Document: https://docs.google.com/presentation/d/1RtGy4zgxBG-H7wGXw8zTPCeFQ0AyTzmp0xpOUroDoJs/edit?usp=sharing
## Process to get started
Before getting started, one important note that - I don't use local file system to store generated dart. I stored dart value into Hadoop file system directly.
- Create directory "pi-calculation" into home directory "mkdir pi-calculation"
- Change directory to pi-calculation "cd pi-calculation"
- Create source directory called "src" and change to source directory i.e. mkdir and cd to source
- Put [Pi calculation source code](Pi.java) into Pi.java file
- Optionally create [compile](compile.sh) and [re-run](re-run.sh) shell script which make repeated work easy!
## Compile Java code
To compile Java code we need to change our directory to java source code directory otherwise "ClassNotFound" exception may raise.
```
$ cd ~/pi-calculation/src
$ ~/hadoop-3.3.4/bin/hadoop com.sun.tools.javac.Main Pi.java
$ jar cf pi.jar Pi*.class
$ mv pi.jar ..
```
## Execute our Pi with Hadoop
```
$ ~/hadoop-3.3.4/bin/hdfs namenode -format
$ ~/hadoop-3.3.4/sbin/start-dfs.sh
$ ~/hadoop-3.3.4/bin/hdfs dfs -mkdir /user
$ ~/hadoop-3.3.4/bin/hdfs dfs -mkdir /user/shkr
$ ~/hadoop-3.3.4/bin/hdfs dfs -mkdir /user/shkr/pi-calculation
$ ~/hadoop-3.3.4/bin/hadoop jar ~/pi-calculation/pi.jar Pi /user/shkr/pi-calculation
```
Here "shkr" is my user ID, replace this with your ID.

