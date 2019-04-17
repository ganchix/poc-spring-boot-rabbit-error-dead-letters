package io.github.ganchix.rabbitdeadletter;

import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;

@SpringBootApplication
public class RabbitDeadLetterApplication {

    public static void main(String[] args) {
        SpringApplication.run(RabbitDeadLetterApplication.class, args);
    }

    @Bean
    public Queue inputQueue() {
        return QueueBuilder.durable("InputQueue").build();
    }

    @Bean
    public Queue inputLongQueue() {
        return QueueBuilder.durable("InputLongQueue").build();
    }

    @Bean
    @DependsOn(value = "amqpAdmin")
    public RabbitListenerErrorHandler retryRabbitListenerErrorHandler(RabbitTemplate rabbitTemplate) {
        return new RetryRabbitListenerErrorHandler
                .Builder(10000L)
                .addQueueDeadLetter("InputLongQueue", 30000L)
                .withRabbitTemplate(rabbitTemplate)
                .build();
    }


}
