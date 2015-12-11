#!/bin/bash

# VARIABLES
HADOOP_HOME=/home/$USER/hadoop-1.2.1
WORKING_DIR=/home/$USER/Exercises-PAD/connectedComponents
JAR_PATH=target/connectedComponents-1.0-SNAPSHOT.jar
DATASET=$WORKING_DIR/data

if [ $# != 3 ]; then
	echo "Usage: $0 base_input final_output_file correct_file"
	exit 1
fi

base_input=$1
final_output_file=$2
correct_file=$3

# sort the final_output_file
sort $DATASET/$final_output_file > $DATASET/tmp.txt
rm $DATASET/$final_output_file
mv $DATASET/tmp.txt $DATASET/$final_output_file

# Compare the result of the Job with the expected result.
num_differences=$(diff $DATASET/$correct_file $DATASET/$final_output_file | wc -l)
if [ $num_differences != 0 ]; then
	echo -e "\033[1;31mFailed to process $base_input !\033[0m"
	echo "Compare $DATASET/$correct_file with $DATASET/$final_output_file for more details."
	exit 1
else
	echo "No differences between $correct_file and $final_output_file."
fi

# Delete the output file since the test is been correctly compleated
rm $DATASET/$final_output_file
echo "Deleted $DATASET/$final_output_file"
