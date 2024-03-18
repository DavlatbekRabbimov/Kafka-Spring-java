package apache.kafka.config;

import apache.kafka.model.MessageEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;

@RequiredArgsConstructor
@Configuration
public class MessageConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String server;

    @Value("${app.kafka.group-id}")
    private String groupId;


    @Bean
    public ProducerFactory<String, MessageEntity> msgProducerFactory(ObjectMapper objectMapper){

        HashMap<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);


        return new DefaultKafkaProducerFactory<>(
                config,
                new StringSerializer(),
                new JsonSerializer<>(objectMapper)
        );
    }

    @Bean
    public KafkaTemplate<String, MessageEntity> kafkaTemplate(
            ProducerFactory<String, MessageEntity> producerFactory){
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, MessageEntity> consumerFactory(ObjectMapper objectMapper){

        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                new JsonDeserializer<>(objectMapper));

    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageEntity> containerFactory(
            ConsumerFactory<String, MessageEntity> consumerFactory
    ){
        ConcurrentKafkaListenerContainerFactory<String, MessageEntity> concurrentContainer =
                new ConcurrentKafkaListenerContainerFactory<>();

        concurrentContainer.setConsumerFactory(consumerFactory);
        return concurrentContainer;
    }

}
