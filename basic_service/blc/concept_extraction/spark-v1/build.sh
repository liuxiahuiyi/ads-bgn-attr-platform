#! /bin/sh

loginfo() { echo "[INFO] $@"; }
logerror() { echo "[ERROR] $@" 1>&2; }

################################################################################
loginfo "sbt assembly"
sbt assembly
if [ $? -ne 0 ]; then
  logerror "sbt assembly failed."
  exit 1
fi

################################################################################
loginfo "cp package jar to output"
rm -rf output
mkdir output

PACKAGE_JAR_NAME=bgn-blc-assembly-0.1.0.jar
PACKAGE_JAR=./target/scala-2.11/${PACKAGE_JAR_NAME}

cp ${PACKAGE_JAR} ./output/
if [ $? -ne 0 ]; then
  logerror "mv package jar failed."
  exit 1
fi 

################################################################################
loginfo "cp bin to output"
cp bin/*.sh ./output/
if [ $? -ne 0 ]; then
    logerror "cp bin failed."
    exit 1
fi

################################################################################
loginfo "zip output"
rm bgn_blc.zip
zip -r bgn_blc.zip ./output/*
if [ $? -ne 0 ]; then
    logerror "zip and cp output failed"
    exit 1
fi


loginfo "build success"

