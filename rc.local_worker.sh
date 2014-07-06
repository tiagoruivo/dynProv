#!/bin/sh
cd /home/ubuntu/PA4
javac -classpath .:lib/* Worker.java
#parameter is number of miliseconds idle until shutting off
java -classpath .:lib/* Worker 60000
exit 0