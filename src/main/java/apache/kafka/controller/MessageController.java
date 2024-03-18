package apache.kafka.controller;

import apache.kafka.model.MessageEntity;
import apache.kafka.service.MessageService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RequiredArgsConstructor
@RestController
@RequestMapping("api")
public class MessageController {

    @Value("${app.kafka.topic}")
    private String topic;

    private final MessageService messageService;

    private final KafkaTemplate<String, MessageEntity> kafkaTemplate;

    @PostMapping("/send-message")
    public ResponseEntity<String> sendMessage(@RequestBody MessageEntity message){
        String key = UUID.randomUUID().toString();
        kafkaTemplate.send(topic, key, message);
        return ResponseEntity.ok("OK: Message sent successfully!");
    }

    @GetMapping("/message/{id}")
    public ResponseEntity<String> getById(@PathVariable Long id){
        try {
            return ResponseEntity.ok(messageService.getById(id).toString());
        } catch (Exception e){
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error: Message ID is not found!");
        }

    }

    @PostMapping("/delete/{id}")
    public ResponseEntity<String> deleteMessage(@PathVariable Long id){
        messageService.deleteById(id);
        return ResponseEntity.ok("OK: Message is deleted successfully!");
    }

}
