#!/bin/bash

VENV_DIR=ethereum_pyspark

if [ ! -d $VENV_DIR ]; then
    echo "Virtualenv doesn't exist. Creating one"
    virtualenv ethereum_pyspark
    #echo $VENV_DIR/Scripts/activate
    source $VENV_DIR/Scripts/activate
    pip install -r requirements.txt
  else
    echo "fuck off"
    #echo $VENV_DIR/Scripts/activate
    source $VENV_DIR/Scripts/activate
fi


./ethereum_pyspark/Lib/site-packages/pyspark/bin/pyspark \
--master local[4] \
--py-files ethereum_effect_pyspark/ethereum_effect_on_cumputer_parts.py

deactivate