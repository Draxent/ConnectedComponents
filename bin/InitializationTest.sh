#!/bin/bash

source config.txt

for original_input in $DATASET/input*
do
	# Skip garbage
	if [ "${original_input: -1}" == "~" ]; then
		continue
	fi

	base_input=$(basename $original_input)
	input="${base_input%.*}"
	number=${input#i*_}
	output="out${number}"
	echo "Processing $base_input."

	# Insert $input in the hadoop distibuted file system
	$HADOOP fs -put $original_input $input
	echo "Added hdfs://localhost:9000/user/$USER/$input"

	# Start the Job and check if it is compleated correctly
	echo "InitializationDriver Job started !"
	result=$($HADOOP jar $JAR_PATH pad.InitializationDriver $input $output 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in InitializationDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "InitializationDriver Job completed correctly !"
	fi

	# Start the Job and check if it is compleated correctly
	echo "TranslatorDriver Pair2Text Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TranslatorDriver Pair2Text $output ${output}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TranslatorDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TranslatorDriver Pair2Text Job completed correctly !"
	fi

	correct_file="init_${number}.txt"
	final_output_file="init_out_${number}.txt"

	# Merge the results of the Job and copy the output file locally
	$HADOOP fs -getmerge ${output}T $DATASET/$final_output_file

	# Clean file on hadoop
	$HADOOP fs -rmr $input
	$HADOOP fs -rmr $output
	$HADOOP fs -rmr ${output}T

	$WORKING_DIR/bin/compare_result.sh $base_input $final_output_file $correct_file
	if [ $? != 0 ]; then
		exit 1
	fi

	echo -e "\033[1;92mTest on $base_input compleated correctly !\033[0m"
done
