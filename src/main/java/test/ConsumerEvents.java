package test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Component;

@Component("consumer")
public class ConsumerEvents {
	
	private Log log = LogFactory.getLog(getClass());
	
	private ObjectMapper mapper = new ObjectMapper();

	@PostConstruct
	public void startConsuming() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "migrupo");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("metadata.max.age.ms", "2000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		new Thread(() -> {			
			try(KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props))
			{
				consumer.subscribe(Pattern.compile(System.getProperty("pattern", "alarms(.*)")), new ConsumerRebalanceListener() {
					@Override
					public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
						log.debug(Arrays.toString(partitions.toArray()) + " topic-partitions are revoked from this consumer");
					}
		
					@Override
					public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
						log.debug(Arrays.toString(partitions.toArray()) + " topic-partitions are assigned to this consumer");
					}
				});
				
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(100);
					for (ConsumerRecord<String, String> record : records)
					{
						try {
							Map<?, ?> message = mapper.readValue(record.value(), Map.class);
							log.info("Consuming : " + message);
						} 
						catch (Exception e) {
							e.printStackTrace();
						}
					}
				}			
			}
		}).start();
	}
}