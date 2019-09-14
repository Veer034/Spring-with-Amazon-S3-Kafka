package com.aws.s3.writer;

import com.amazonaws.services.s3.AmazonS3;
import com.aws.s3.config.KakfaConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class S3DataWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3DataWriter.class);

    @Autowired
    private KakfaConfiguration kakfaConfiguration;

    @Value("${app.aws.s3.bucketname}")
    private String bucketName;

    @Value("${app.aws.s3.folder}")
    private String folder;

    /**
     * This method will read the message from kafka topics and produce into aws S3, If the folder is not in bucket then
     * new folder will get created.
     * @param cr : Contains all the information of the message with all the extra information
     * @param payload : Contains the message.
     */
    @KafkaListener(topics = "aws.write", clientIdPrefix = "string", groupId = "aws",
            containerFactory = "kafkaListenerContainerFactory")
    public void listenasString(ConsumerRecord<String, String> cr, @Payload String payload) {
        LOGGER.info("Consuming message form topics:{} , contents:{}", cr.topic(), cr.value());
        try {
            JSONObject obj = new JSONObject(cr.value());
            AmazonS3 s3Client =kakfaConfiguration.awsS3Client();
            String uniqueFileName = obj.getString("sessionId") + "_" + obj.getString("eventTime");
            Instant instant = Instant.now();
            ZonedDateTime z = instant.atZone(ZoneId.of("UTC"));
            DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyyMMddhhmm");
            String timeZone = fmt.format(z);
            s3Client.putObject(bucketName+"/"+folder+"/"+timeZone, uniqueFileName, cr.value());
        } catch (Exception ex) {
            LOGGER.info("Exception while putting message:{} to amazon S3. Exception:{}", cr.value(),ex);
        }
    }

}