# Spring-with-Amzon-S3-Kafka
This project contains basic operation of producing events in kafka topic, Read the value from topic and store in amazon S3, Then provide api to fetch the data back from S3 bucket and push into another kafka topic.

This project contains code regarding stream,join of data coming from various 
sources, such as file,socket & kafka.

Installation :<br>
Follow these documents for setup kafka environment.<br>
https://www.sohamkamani.com/blog/2017/11/22/how-to-install-and-run-kafka/

#### Build Command:
    mvn clean install
 This command will create .jar file in target directory. 
 
#### KafkaMessageProducer.java :
    contains logic to produce the data by reading from s3data file number of times provided by user, and put it into kafka
    topic "aws.read".

#### S3DataWriter.java:
    contains logic to write the data into amazon s3, @KafkaListener has been used to listing from the topic. Amazon has 
    provided aws-sdk for perorming operation on the aws applications. This class store the incoming data into different 
    partion based on the time.

Data stored in S3 :<br>
<img src="images/Data in S3.png" width=800 height=400><br/>

#### S3DataReader.java:
    contains logic of reading from the amazon s3 using aws-sdk dependencies. First the summury will be fetched for the passed
    timestamp, then get operation will be performed based on key. After that data will be produced to another kafka topic"aws.write".
    
Data read from S3 and put into kafka :<br>
<img src="images/Kafka listener.png" width=800 height=400><br/>


### Note : 
For using this user must register to amazon account, then create secreat key and access key. Along with these info proper 
bucket, folder and client-region information is also needed.
