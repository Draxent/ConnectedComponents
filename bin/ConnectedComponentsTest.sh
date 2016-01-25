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
	echo "ConnectedComponents Job started !"
	cc_result=$($HADOOP jar $JAR_PATH pad.ConnectedComponents $input $output 2>&1)
	cc_out=$?
	if [ $cc_out == 1 ]; then
		echo -e "\033[1;31mError in ConnectedComponents Job !\033[0m"
		exit 1
	fi
	echo "ConnectedComponents Job completed !"
	echo ${cc_result#*End ConnectedComponents.*} | sed 's/\. /.\n/g'

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
	$HADOOP fs -rmr $output
	$HADOOP fs -rmr ${output}T

	if [ $cc_out == 2 ]; then
		# sort the final_output_file
		sort $DATASET/$final_output_file > $DATASET/${final_output_file}_tmp.txt
		rm $DATASET/$final_output_file
		mv $DATASET/${final_output_file}_tmp.txt $DATASET/$final_output_file

		echo -e "\033[1;31mTest on $base_input is failed: at least one cluster is malformed !\033[0m"
		echo "Check $final_output_file for more details."
		exit 1
	fi

	$WORKING_DIR/bin/compare_result.sh $base_input $final_output_file $correct_file
	if [ $? != 0 ]; then
		exit 1
	fi

	echo -e "\033[1;92mTest on $base_input compleated correctly !\033[0m"
done
