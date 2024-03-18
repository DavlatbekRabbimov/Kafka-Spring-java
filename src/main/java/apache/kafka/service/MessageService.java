package apache.kafka.service;

import apache.kafka.model.MessageEntity;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@AllArgsConstructor
@Service
public class MessageService {

    private final Set<MessageEntity> messageRepo = new HashSet<>();

    public void addMessage(MessageEntity message){
        messageRepo.add(message);
    }

    public void deleteById(Long id){
       Optional<MessageEntity> message = getById(id);
       message.ifPresent(messageRepo::remove);
    }

    public Optional<MessageEntity> getById(Long id){
        return Optional.ofNullable(messageRepo.stream()
                .filter(message -> message.getId().equals(id))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Error: MessageEntity ID: " + id + " is not found!")));
    }

}
