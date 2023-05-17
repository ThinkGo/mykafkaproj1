package a;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // 1. take all properties
        Properties properties =new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"1234");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        KafkaProducer<String,String> producer= new KafkaProducer<>(properties);

        ProducerRecord record=new ProducerRecord("tuesday","India","is my home");
        producer.send(record);
        producer.close();
        System.out.println("sent msg run below to check");
        System.out.println("bin/kafka-console-consumer.sh --topic  tuesday  --from-beginning --bootstrap-server localhost:9092");
        System.out.println("./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic tuesday --property print.key=true");


//        ProducerRecord
        // 2. object of producer class
                //create record
        // 3. biz logic > pass data
        // 4. close


    }

}
