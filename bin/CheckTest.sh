#!/bin/bash

source config.txt

for original_input in $DATASET/cluster*
do
	# Skip garbage
	if [ "${original_input: -1}" == "~" ]; then
		continue
	fi

	base_input=$(basename $original_input)
	input="${base_input%.*}"
	number=${input#t*_}
	output="out${number}"
	echo "Processing $base_input."

	# Insert $input in the hadoop distibuted file system
	$HADOOP fs -put $original_input $input
	echo "Added hdfs://localhost:9000/user/$USER/$input"

	# Start the Job and check if it is compleated correctly
	echo "TranslatorDriver Text2Cluster Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TranslatorDriver Text2Cluster ${input} ${input}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TranslatorDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TranslatorDriver Text2Cluster Job completed correctly !"
	fi

	# Start the Job and check if it is compleated correctly
	echo "CheckDriver Job started !"
	result=$($HADOOP jar $JAR_PATH pad.CheckDriver ${input}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in CheckDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "CheckDriver Job completed correctly !"
	fi

	# Clean file on hadoop
	$HADOOP fs -rmr $input
	$HADOOP fs -rmr ${input}T

	if [[ "$result" == *"TestOK: true"* ]]; then
		echo -e "\033[1;92mTest on $base_input compleated correctly !\033[0m"
	else
		echo -e "\033[1;31mTest on $base_input is failed: at least one cluster is malformed !\033[0m"	
	fi
done
