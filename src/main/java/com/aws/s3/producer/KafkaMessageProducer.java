package com.aws.s3.producer;

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
import java.io.FileReader;
import java.util.*;

@RestController
public class KafkaMessageProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${kafka.write.topic.name}")
    private String topicName;

    @Value("${data.path}")
    private String filePath;

    /**
     * This method will produce the data in kafka using the count of event user passed in the parameter.
     * We are generating some random data with same sessionId for the events.
     * @param count : This contains number times request should be made.
     */
    @RequestMapping(value = "/api/push/{count}", method = {RequestMethod.GET})
    public String publish(@PathVariable Integer count) {
        LOGGER.info("Producing data for kafka topics:{} , count :{}",topicName,count);
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            List<String> csrs = new ArrayList<>();
            while ((line = br.readLine()) != null) {
                csrs.add(line);
            }
            Random rand = new Random();
            for (int i = 1; i <= count; i++) {
                Iterator<String> iterator = csrs.iterator();
                String sessionId = UUID.randomUUID().toString();
                StringBuffer sb= new StringBuffer();
                while (iterator.hasNext()) {
                    String entity = UUID.randomUUID().toString();
                    String agentSessionId = UUID.randomUUID().toString();
                    Thread.sleep(100);
                    String text = iterator.next().replace("$sessionId", sessionId).
                            replace("$tenantId", ""+rand.nextInt(5)).
                            replace("$entityId", entity).replace("$agentSessionId",agentSessionId)
                            .replace("$eventTime", "" + System.currentTimeMillis());
                    kafkaTemplate.send(topicName, sessionId, text);
                    sb.append(text);
                }
                LOGGER.info("Data post in topics:{} , message:{}",topicName,sb.toString());
            }
        } catch (Exception ex) {
            System.err.println("Exception while creating data for input topics  :" + ex);
            return "Exception Occurred.";
        }
        return "Successfully Applied.";
    }
}
