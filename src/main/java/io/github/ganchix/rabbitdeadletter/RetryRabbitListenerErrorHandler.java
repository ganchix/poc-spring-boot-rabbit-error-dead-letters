package io.github.ganchix.rabbitdeadletter;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.listener.exception.ListenerExecutionFailedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RetryRabbitListenerErrorHandler implements RabbitListenerErrorHandler {

    private static final String MESSAGE_TTL = "x-message-ttl";
    private static final String DEAD_LETTER_ROUTING_KEY = "x-dead-letter-routing-key";
    private static final String DEAD_LETTER_EXCHANGE = "x-dead-letter-exchange";

    private long defaultDeadLetter;
    private Map<String, Long> queueWithDeadLetter;
    private String exchangeName = "errorExchange";
    private String errorQueueSuffix = "_ERROR";
    private RabbitAdmin rabbitAdmin;
    private Exchange generalExchange;
    private RabbitTemplate rabbitTemplate;

    private RetryRabbitListenerErrorHandler() {
    }


    private void init() {
        rabbitAdmin = new RabbitAdmin(rabbitTemplate);
        generalExchange = ExchangeBuilder.directExchange(exchangeName).build();
        rabbitAdmin.declareExchange(generalExchange);
        queueWithDeadLetter.forEach((queue, deadLetter) -> {
            if (rabbitAdmin.getQueueProperties(queue) == null) {
                throw new IllegalStateException("Queue " + queue + " is not defined");
            }
            if (rabbitAdmin.getQueueProperties(queue.concat(errorQueueSuffix)) == null) {
                createQueueAndBinding(queue, deadLetter);
            }
        });
    }

    @Override
    public Object handleError(Message amqpMessage, org.springframework.messaging.Message<?> message, ListenerExecutionFailedException exception) throws Exception {

        String queueName = amqpMessage.getMessageProperties().getConsumerQueue();
        if (rabbitAdmin.getQueueProperties(queueName.concat(errorQueueSuffix)) == null) {
            createQueueAndBinding(queueName, defaultDeadLetter);
        }
        rabbitTemplate.convertAndSend(queueName.concat(errorQueueSuffix), amqpMessage.getBody());

        return null;
    }

    private void createQueueAndBinding(String queueName, Long deadLetter) {
        Queue queue = QueueBuilder.durable(queueName.concat(errorQueueSuffix))
                .withArgument(MESSAGE_TTL, deadLetter)
                .withArgument(DEAD_LETTER_ROUTING_KEY, queueName)
                .withArgument(DEAD_LETTER_EXCHANGE, exchangeName)
                .build();

        Binding binding = BindingBuilder.bind(QueueBuilder.durable(queueName).build())
                .to(generalExchange)
                .with(queueName)
                .noargs();

        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(binding);
    }


    public static class Builder {
        private long defaultDeadLetter;
        private Map<String, Long> queueWithDeadLetter = new HashMap();
        private Optional<String> exchangeName = Optional.empty();
        private Optional<String> errorQueueSuffix = Optional.empty();
        private RabbitTemplate rabbitTemplate;

        public Builder(long defaultDeadLetter) {
            this.defaultDeadLetter = defaultDeadLetter;
        }

        public Builder withRabbitTemplate(RabbitTemplate rabbitTemplate) {
            this.rabbitTemplate = rabbitTemplate;
            return this;
        }

        public Builder addQueueDeadLetter(String queueName, long deadLetter) {
            queueWithDeadLetter.put(queueName, deadLetter);
            return this;
        }

        public Builder withExchangeName(String exchangeName) {
            this.exchangeName = Optional.of(exchangeName);
            return this;
        }

        public Builder withErrorQueueSuffix(String errorQueueSuffix) {
            this.errorQueueSuffix = Optional.of(errorQueueSuffix);
            return this;
        }


        public RetryRabbitListenerErrorHandler build() {
            if (rabbitTemplate == null) {
                throw new IllegalStateException("Include Rabbit Template");
            }
            RetryRabbitListenerErrorHandler errorHandler = new RetryRabbitListenerErrorHandler();
            errorHandler.defaultDeadLetter = this.defaultDeadLetter;
            errorHandler.queueWithDeadLetter = this.queueWithDeadLetter;
            errorHandler.rabbitTemplate = this.rabbitTemplate;
            this.exchangeName.ifPresent(exchangeName -> errorHandler.exchangeName = exchangeName);
            this.errorQueueSuffix.ifPresent(errorQueueSuffix -> errorHandler.errorQueueSuffix = errorQueueSuffix);
            errorHandler.init();
            return errorHandler;
        }
    }
}
