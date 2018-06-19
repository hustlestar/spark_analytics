#!/bin/bash

VENV_DIR=ethereum_pyspark

if [ ! -d ${VENV_DIR} ]; then
    echo "Virtualenv doesn't exist. Creating one"
    virtualenv ethereum_pyspark
    #echo $VENV_DIR/Scripts/activate
    source ${VENV_DIR}/Scripts/activate
    pip install -r requirements.txt
  else
    echo "Venv exists. Activating."
    source ${VENV_DIR}/Scripts/activate
fi

#In the following variable Spark is stored, also winutils.exe are stored there at ./bin dir
WINDOWS=/c/Users/Yauheni_Malashchytsk/spark-2.3.0-bin-hadoop2.7

export HADOOP_HOME=${WINDOWS}

#Remove .cmd when on Linux
${SPARK_HOME}/bin/spark-submit.cmd \
--master local[*] \
--deploy-mode client \
ethereum_effect_pyspark/ethereum_effect_on_cumputer_parts.py

echo "Job finished deactivating Venv."
deactivate