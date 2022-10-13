#!/usr/bin/env bash
~/hadoop-3.3.4/bin/hadoop com.sun.tools.javac.Main Pi.java
jar cf pi.jar Pi*.class
mv pi.jar ..


