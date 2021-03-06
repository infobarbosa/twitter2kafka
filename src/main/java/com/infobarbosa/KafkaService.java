package com.infobarbosa;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.infobarbosa.EnvironmentSetupException;

public class KafkaService{
   private Properties props;

   private static KafkaService kafkaService;
   private static Producer producer;
   private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
   private static final String KAFKA_TOPIC = System.getenv("KAFKA_TOPIC");

   private KafkaService() throws EnvironmentSetupException{
      if( KAFKA_BOOTSTRAP_SERVERS == null ){
          throw new EnvironmentSetupException("Variável de ambiente não setada: KAFKA_BOOTSTRAP_SERVERS");
      }

      if( KAFKA_TOPIC == null ){
          throw new EnvironmentSetupException("Variável de ambiente não setada: KAFKA_TOPIC");
      }

      Properties props = new Properties();
      props.put("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS );
      props.put("acks", "all");
      props.put("retries", 0);
      props.put("batch.size", 16384);
      props.put("linger.ms", 1);
      props.put("buffer.memory", 33554432);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

      producer = new KafkaProducer<>(props);

   }

   public static KafkaService getInstance() throws EnvironmentSetupException{
      if( kafkaService == null ){
         kafkaService = new KafkaService();
      }
      
      return kafkaService;
   }

   public void enqueue(String tweetId, String tweet){
      producer.send(new ProducerRecord<String, String>(KAFKA_TOPIC, tweetId, tweet));
   }

   public void close(){
     if( producer != null ){
        producer.close();
     }
   }
}
