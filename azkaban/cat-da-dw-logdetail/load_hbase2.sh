#!/bin/sh
source /etc/profile && spark-submit --master yarn --deploy-mode cluster --class com.jianbing.main.ExtraIdCount ExtraIdCount-jar-with-dependencies.jar