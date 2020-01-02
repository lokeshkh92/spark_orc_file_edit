# spark_orc_file_edit

The file can be used to edit column in an orc file and save the orc file with similar partition column and to a new location

Values to be replaced as per the requirement:
1. basepath of orc file: it's the basepath from where orc file to be edited is present
2. column_name: it's the name of the column to be edited
3. value to be replaced: it's the value of the column to be replaced
4. value which is to be replaced: it's the value with which the value in point#3 to be replaced
5. basepath where orc file to be written: it's the basepath where the new edited orc file is to be written

Software Dependencies:
1. Spark version Spark-2.2 or above.
2. Python-2.7 or above


##################################################################################

Below is the command to submit the spark job:

spark2-submit --master yarn --deploy-mode client --executor-memory=4g --num-executors=3 --executor-cores=2 --driver-memory=2g orc_filre_edit.py
