package com.aws.s3.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KakfaConfiguration {

	@Value("${app.aws.iam.accesskey}")
	private String acessKey;

	@Value("${app.aws.iam.secretkey}")
	private String secretKey;

	@Value("${app.aws.s3.clientregion}")
	private String clientRegion;


	@Bean
	public AmazonS3 awsS3Client() {
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(acessKey, secretKey);
		return AmazonS3ClientBuilder.standard().withRegion(clientRegion)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
	}


	/**
	 * Configuration of kafka producer factory, with port number, serializer and deserializer.
	 * @return
	 */
	@Bean
	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	public KafkaTemplate<String, String > kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	/**
	 * Configuration of kafka consumer factory, with port number, serializer and deserializer.
	 * @return
	 */
	@Bean
	public ConsumerFactory<String, Object> consumerFactory() {
		final JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
		jsonDeserializer.addTrustedPackages("*");
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), jsonDeserializer
		);
	}

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());

		return factory;
	}
}
