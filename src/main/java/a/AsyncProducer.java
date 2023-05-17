package a;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AsyncProducer {
    public static void main(String[] args) {
        // 1. take all properties
        Properties properties =new Properties();
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"1234");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG,"1");
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"3"); //BATCH ACK BACK FROM CLUSTER TO PRODUCER

        //
        KafkaProducer<String,String> producer= new KafkaProducer<>(properties);

        final String topic1 = "tuesday";
        final String key1 = "USA";
        final String value1 = "In new york wednesdsay";
        ProducerRecord record=new ProducerRecord(topic1, key1, value1);
        try {
//            final RecordMetadata recordMetadata = (RecordMetadata) producer.send(record).get();
            final MyClass myClass = new MyClass();
            Thread.currentThread().setName(" AsyncProducer :: main");

            producer.send(record, myClass);
            System.out.println("main thread name is ="+Thread.currentThread().getName());
            //not wait for this line to execute //for single message
        } catch (Exception e) {
            e.printStackTrace();;
        }finally {
            producer.close();
        }
    }
}

class MyClass implements Callback {

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e != null) {
            e.printStackTrace();
        }else {
            System.out.println("MyClass onCompletion thread name="+Thread.currentThread().getName());
            System.out.println("response partition no="+recordMetadata.partition());
            System.out.println("recordMetadata offset="+recordMetadata.offset());
            System.out.println("recordMetadata ts="+recordMetadata.timestamp());
        }


    }
}