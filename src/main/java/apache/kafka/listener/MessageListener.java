package apache.kafka.listener;

import apache.kafka.model.MessageEntity;
import apache.kafka.service.MessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Component
public class MessageListener {

    private final MessageService messageService;

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${app.kafka.group-id}",
            containerFactory = "containerFactory"
    )

    public void listener(
            @Payload MessageEntity message,
            @Header(value = KafkaHeaders.RECEIVED_KEY) UUID key,
            @Header(value = KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(value = KafkaHeaders.RECEIVED_PARTITION) Integer partition,
            @Header(value = KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp
            ){
        log.info("Message: {}", message);
        log.info("Key: {}", key);
        log.info("Topic: {}", topic);
        log.info("Partition: {}", partition);
        log.info("Timestamp: {}", timestamp);

        messageService.addMessage(message);
    }

}
