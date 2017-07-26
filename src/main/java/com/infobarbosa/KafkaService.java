package com.infobarbosa;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class KafkaService{
   private Properties props;

   private static KafkaService kafkaService;
   private static Producer producer;

   private KafkaService(){
      Properties props = new Properties();
      props.put("bootstrap.servers", "localhost:9092");
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("linger.ms", 1);
      props.put("buffer.memory", 33554432);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      producer = new KafkaProducer<>(props);

   }

   public static KafkaService getInstance(){
      if( kafkaService == null ){
         kafkaService = new KafkaService();
      }
      
      return kafkaService;
   }

   public void enqueue(String tweet){
      producer.send(new ProducerRecord<String, String>("tweets", null, tweet));
   }

   public void close(){
     if( producer != null ){
        producer.close();
     }
   }
}
