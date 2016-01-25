#!/bin/bash

# VARIABLES TO SET
HADOOP_HOME=/home/$USER/hadoop-1.2.1
WORKING_DIR=/home/$USER/Exercises-PAD/connectedComponents2

# DERIVATE VARIABLES
JAR_PATH=$WORKING_DIR/target/connectedComponents-1.0-SNAPSHOT.jar
HADOOP=$HADOOP_HOME/bin/hadoop
DATASET=$WORKING_DIR/data

if [ $# != 1 ]; then
	echo "Usage: $0 type"
	exit 1
fi

# Fix the type input to ensure it is the word "small" or "large"
type=$(echo $1 | tr '[:upper:]' '[:lower:]')
if [ "$type" != "large" ]; then
	type="small"
else
	type="large"
fi

for original_input in $DATASET/init*
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
	echo "TranslatorDriver Text2Pair Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TranslatorDriver Text2Pair ${input} ${input}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TranslatorDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TranslatorDriver Text2Pair Job completed correctly !"
	fi

	# Start the Job and check if it is compleated correctly
	echo "StarDriver (type: $type) Job started !"
	result=$($HADOOP jar $JAR_PATH pad.StarDriver $type ${input}T $output 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in StarDriver (type: $type) Job :\033[0m"; echo $result; exit 1
	else
		echo "StarDriver (type: $type) Job completed correctly !"
	fi

	# Start the Job and check if it is compleated correctly
	echo "TranslatorDriver Pair2Text Job started !"
	result=$($HADOOP jar $JAR_PATH pad.TranslatorDriver Pair2Text $output ${output}T 2>&1)
	if [[ "$result" != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TranslatorDriver Job :\033[0m"; echo $result; exit 1
	else
		echo "TranslatorDriver Pair2Text Job completed correctly !"
	fi

	correct_file="${type}-star_${number}.txt"
	final_output_file="${type}-star_out_${number}.txt"

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
