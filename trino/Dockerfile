FROM trinodb/trino:435

# Copy Hadoop AWS JARs to Hive plugin directory
COPY jars/hadoop-aws-3.3.4.jar /usr/lib/trino/plugin/hive/
COPY jars/aws-java-sdk-bundle-1.12.262.jar /usr/lib/trino/plugin/hive/

USER trino
