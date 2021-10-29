# Spark S3 Demo

This is a demo of Apache Spark reading from S3 and writing to S3.

When running the main class, the script of Shakespeare's Macbeth will be read from S3, word count will be calculated and then written as a CSV to S3.

## Set Up

1. Install Hadoop and make sure to change the `HADOOP_HOME_DIR` to your Hadoop installation path and make sure it matches the `hadoop-common` version specified in the `pom.xml`.
2. Set up a MinIO instance and create a bucket named `example-data`.\
3. Upload the `macbeth.txt` file from the resources folder to the `example-data` bucket.
4. Change the values of `S3_ENDPOINT`, `S3_ACCESS_KEY` and `S3_SECRET_KEY` to your S3 instance.
5. Run `org.peleg.Main` class.

## Result

After running the program successfully, a folder named `macbeth-word-counts` was created in the `example-data` bucket containing a CSV file with the word counts of the Macbeth script.

It should look like this:
```csv
"",4904
the,607
and,415
I,364
to,330
of,320
MACBETH,269
a,215
s,199
you,183
in,177
...
...
```

## Warning

This code was tested locally on Windows with MinIO.