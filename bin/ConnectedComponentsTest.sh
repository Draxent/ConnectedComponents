#!/bin/bash

# VARIABLES
HADOOP_HOME=/home/$USER/hadoop-1.2.1
WORKING_DIR=/home/$USER/Exercises-PAD/connectedComponents
JAR_PATH=target/connectedComponents-1.0-SNAPSHOT.jar
DATASET=$WORKING_DIR/data

for input in $DATASET/input*
do
	# Skip garbage
	if [ "${input: -1}" == "~" ]; then
		continue
	fi

	base_input=$(basename $input)
	pure_input="${base_input%.*}"
	number=${pure_input#i*_}
	output_dir="${pure_input}_out"
	echo "Processing $base_input."

	# Insert $input in the hadoop distibuted file system
	$HADOOP_HOME/bin/hadoop fs -put $input $base_input
	echo "Added hdfs://localhost:9000/user/$USER/$base_input"

	# Start the Job
	echo "ConnectedComponents Job started !"
	result="$($HADOOP_HOME/bin/hadoop jar $WORKING_DIR/$JAR_PATH test.ConnectedComponentsTest $base_input 2>&1)"
	echo "ConnectedComponents Job completed !"
	
	correct_file="final_output_${number}.txt"
	final_output_file="final_output_out_${number}.txt"

	# Write results on final_output_file
	res="${result#*Cluster}"
	echo "Cluster$res" | sed 's/}/}\n/g' | grep -e "Cluster" > $DATASET/$final_output_file

	# Clean file on hadoop
	$HADOOP_HOME/bin/hadoop fs -rmr $base_input

	./compare_result.sh $base_input $final_output_file $correct_file
	if [ $? != 0 ]; then
		exit 1
	fi

	echo -e "\033[1;92mTest on $base_input compleated correctly !\033[0m"
done
