#!/bin/bash

# VARIABLES
HADOOP_HOME=/home/$USER/hadoop-1.2.1
WORKING_DIR=/home/$USER/Exercises-PAD/connectedComponents
JAR_PATH=target/connectedComponents-1.0-SNAPSHOT.jar
DATASET=$WORKING_DIR/data

for input in $DATASET/term*
do
	# Skip garbage
	if [ "${input: -1}" == "~" ]; then
		continue
	fi

	base_input=$(basename $input)
	pure_input="${base_input%.*}"
	number=${pure_input#t*_}
	output_dir="out${number}"
	echo "Processing $base_input."

	# Insert $input in the hadoop distibuted file system
	$HADOOP_HOME/bin/hadoop fs -put $input $base_input
	echo "Added hdfs://localhost:9000/user/$USER/$base_input"

	# Start the Job
	echo "TerminationTest Job started !"
	result="$($HADOOP_HOME/bin/hadoop jar $WORKING_DIR/$JAR_PATH test.TerminationTest $base_input $output_dir 2>&1 > /dev/null)"

	# Check if the Job is compleated correctly
	if [[ $result != *"Job complete"* ]]; then
		echo -e "\033[1;31mError in TerminationTest Job :\033[0m"
		echo $result
		exit 1
	else
		echo "TerminationTest Job completed correctly !"
	fi

	hdfs_outputs=$($HADOOP_HOME/bin/hadoop fs -ls $output_dir | sed '1d;s/  */ /g' | cut -d\  -f8 | grep -e "cluster*" | xargs -n 1 basename)
	# For each cluster produced by the Job	
	for output_file in $hdfs_outputs
	do
		correct_file="output_${number}_${output_file}.txt"
		final_output_file="output_out_${number}_${output_file}.txt"

		# Copy the cluster locally
		$HADOOP_HOME/bin/hadoop fs -get $output_dir/$output_file $DATASET/$final_output_file
		echo "Added $DATASET/$final_output_file"

		./compare_result.sh $base_input $final_output_file $correct_file
		if [ $? != 0 ]; then
			# Clean file on hadoop
			$HADOOP_HOME/bin/hadoop fs -rmr $output_dir
			$HADOOP_HOME/bin/hadoop fs -rmr $base_input
			exit 1
		fi		
	done

	# Clean file on hadoop
	$HADOOP_HOME/bin/hadoop fs -rmr $output_dir
	$HADOOP_HOME/bin/hadoop fs -rmr $base_input

	echo -e "\033[1;92mTest on $base_input compleated correctly !\033[0m"
done