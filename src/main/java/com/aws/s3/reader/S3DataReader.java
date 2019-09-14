package com.aws.s3.reader;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.*;
import com.aws.s3.config.KakfaConfiguration;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

@RestController
public class S3DataReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataReader.class);

    @Autowired
    private KakfaConfiguration kakfaConfiguration;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.read.topic.name}")
    private String topic;

    @Value("${app.aws.s3.bucketname}")
    private String bucketName;

    @Value("${app.aws.s3.folder}")
    private String folder;

    /**
     * Fetch the information from AWS for the timestamp, We can fetch the info from S3 buckets for passed topics.
     * @param timestamp : Contains the timestamp.
     * @return : The data of the bucket folder passed in parameter.
     */
    @RequestMapping(value = "/api/fetch/{timestamp}", method = {RequestMethod.GET},produces = {"application/json"})
    public String fetch(@PathVariable Long timestamp) {
        LOGGER.info("Fetching the info for timestamp:{}", timestamp);
        List<String> summaryList = new ArrayList<>();
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                    .withBucketName(bucketName).withPrefix(folder+"/"+timestamp);
            ObjectListing objectListing;
            do {
                objectListing = kakfaConfiguration.awsS3Client().listObjects(listObjectsRequest);
                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    S3Object s3object = kakfaConfiguration.awsS3Client().getObject(new GetObjectRequest(bucketName, objectSummary.getKey()));
                    BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        summaryList.add(line);
                    }
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());

            kafkaTemplate.send(topic,""+timestamp , summaryList.toString());
        } catch (AmazonServiceException ase) {
            LOGGER.error("Request made to Amazon S3 failed. Error Message : {} \n HTTP Status Code: {} \n AWS Error Code:" +
                    " {} \n Error Type: {} \n Request ID: {}", ase.getMessage(), ase.getStatusCode(), ase.getErrorCode(), ase.getErrorType(), ase.getRequestId());
            throw ase;
        } catch (AmazonClientException ace) {
            LOGGER.error("client encountered an internal error while trying to communicate with S3, such as not being able to access the network. {}", ace.getMessage());
            throw ace;
        } catch (IOException e) {
            LOGGER.error("Exception while reading", e.getMessage());
        }


        return summaryList.toString();
    }
}