FROM tabulario/iceberg-rest:1.6.0

# Add Hadoop AWS libraries
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar /usr/share/java/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /usr/share/java/

# Copy Hadoop configuration
COPY core-site.xml /etc/hadoop/conf/core-site.xml

# Expose port
EXPOSE 8181