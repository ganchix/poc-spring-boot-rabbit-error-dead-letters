package io.github.ganchix.rabbitdeadletter;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class InputListener {

    @RabbitListener(queues = "InputQueue", errorHandler = "retryRabbitListenerErrorHandler")
    public void listener() {
        log.info("Entering in listener");
        throw new RuntimeException();

    }


    @RabbitListener(queues = "InputLongQueue", errorHandler = "retryRabbitListenerErrorHandler")
    public void listenerLong() {
        log.info("Entering in LONG listener");
        throw new RuntimeException();

    }
}
