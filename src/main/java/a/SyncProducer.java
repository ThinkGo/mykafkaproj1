package a;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class SyncProducer {
    public static void main(String[] args) {
        // 1. take all properties
        Properties properties =new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"1234");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG, "4"); //DEFAULT 0
        properties.put(ProducerConfig.ACKS_CONFIG,1);

//        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        KafkaProducer<String,String> producer= new KafkaProducer<>(properties);

        final String topic1 = "tuesday";
        final String key1 = "USA";
        final String value1 = "In new york";
        ProducerRecord record=new ProducerRecord(topic1, key1, value1);
        try {
            final Future send = producer.send(record);
            //GET wait for producer to be successful
            final Object absObj = send.get();//waiting.......
            //if get() success then return response of 1 msg
            //if get() failure it hits Exception
            //response is of type RecordMetadata
            final RecordMetadata recordMetadata = (RecordMetadata) absObj; //
            System.out.println("response partition no="+recordMetadata.partition());
            System.out.println("recordMetadata offset="+recordMetadata.offset());
            System.out.println("recordMetadata ts="+recordMetadata.timestamp());
        } catch (Exception e) {
            e.printStackTrace();;
        }finally {
            producer.close();
        }
//        System.out.println("sent msg run below to check");
//        System.out.println("bin/kafka-console-consumer.sh --topic  tuesday  --from-beginning --bootstrap-server localhost:9092");
//        System.out.println("./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic tuesday --property print.key=true");


//        ProducerRecord
        // 2. object of producer class
                //create record
        // 3. biz logic > pass data
        // 4. close


    }

}
