#!/bin/bash

source config.txt

for original_input in $DATASET/term*
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
	echo "TranslatorDriver Text2Pair Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TranslatorDriver Text2Pair ${input} ${input}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TranslatorDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TranslatorDriver Text2Pair Job completed correctly !"
	fi

	# Start the Job and check if it is compleated correctly
	echo "TerminationDriver Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TerminationDriver ${input}T $output 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TerminationDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TerminationDriver Job completed correctly !"
	fi

	# Start the Job and check if it is compleated correctly
	echo "TranslatorDriver Cluster2Text Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TranslatorDriver Cluster2Text ${output} ${output}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TranslatorDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TranslatorDriver Cluster2Text Job completed correctly !"
	fi

	correct_file="cluster_${number}.txt"
	final_output_file="cluster_out_${number}.txt"

	# Merge the results of the Job and copy the output file locally
	$HADOOP fs -getmerge ${output}T $DATASET/$final_output_file

	# Clean file on hadoop
	$HADOOP fs -rmr $input
	$HADOOP fs -rmr ${input}T
	$HADOOP fs -rmr $output
	$HADOOP fs -rmr ${output}T

	$WORKING_DIR/bin/compare_result.sh $base_input $final_output_file $correct_file
	if [ $? != 0 ]; then
		exit 1
	fi

	echo -e "\033[1;92mTest on $base_input compleated correctly !\033[0m"
done
