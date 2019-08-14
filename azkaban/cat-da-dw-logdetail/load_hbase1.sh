#!/bin/sh
source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.main.LogAnalysis LogAnalysis-jar-with-dependencies.jar