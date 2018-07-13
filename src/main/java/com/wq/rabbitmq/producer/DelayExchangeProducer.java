package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/13
 * @Description:
 * @Resource:
 */
public class DelayExchangeProducer implements ExchangeProducer<String> {

    private RabbitMqBasic delayBasic;
    private RabbitMqBasic deadBasic;
    private int messageTTL;
    private int queueLength;
    private Channel channel;

    private Logger logger = Logger.getLogger(getClass());

    private static final String DELAY_QUEUE_TTL = "x-message-ttl";

    private static final String DELAY_QUEUE_MAX_SIZE = "x-max-length";

    private static final String FORWARD_DEAD_MESSAGE_EXCHANGE = "x-dead-letter-exchange";

    public DelayExchangeProducer(int messageTTL, int queueLength) {
        this.messageTTL = messageTTL;
        this.queueLength = queueLength;
    }

    @Override
    public void bindExchangeAndSend(MQMessageWrapper<String> wrapper, Set<String> routingKeys) {
        logger.info("send message to delay queue");
        try {
            for (String key : routingKeys) {
                this.channel.basicPublish(this.delayBasic.getExchangeName(), key, null, wrapper.serialMessage());
                logger.info("public message [" + wrapper.getMessageBody() + "] with routingkey [" + key + "] to rabbitmq-server's delay queue");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void prepareEnvironment(Channel channel, RabbitMqBasic rabbitMqBasic) {
        logger.debug("prepare delay queue environment");
        this.channel = channel;
        if (rabbitMqBasic != null) {
            throw new RuntimeException("current exchange [delayExchange] doesn't need this value and please change them for setter method");
        }

        try {
            logger.debug("declare dead message first");
            channel.exchangeDeclare(this.deadBasic.getExchangeName(), this.deadBasic.getDelivery().name, true, false, null);
            channel.queueDeclare(this.deadBasic.getQueueName(), true, false, false, null);
            for (String key : this.deadBasic.getRoutingKey()) {
                channel.queueBind(this.deadBasic.getQueueName(), this.deadBasic.getExchangeName(), key);
            }

            logger.debug("declare queue received message at first");
            Map<String, Object> map = new HashMap<>();
            map.put(DELAY_QUEUE_TTL, messageTTL);
            map.put(DELAY_QUEUE_MAX_SIZE, queueLength);
            map.put(FORWARD_DEAD_MESSAGE_EXCHANGE, this.deadBasic.getExchangeName());
            channel.queueDeclare(this.delayBasic.getQueueName(), true, false, false, map);
            channel.exchangeDeclare(this.delayBasic.getExchangeName(), this.delayBasic.getDelivery().name, true, false, null);
            for (String key : this.delayBasic.getRoutingKey()) {
                channel.queueBind(this.delayBasic.getQueueName(), this.delayBasic.getExchangeName(), key);
            }

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

    }

    public void setDelayBasic(RabbitMqBasic rabbitMqBasic) {
        this.delayBasic = rabbitMqBasic;
    }

    public void setDeadBasic(RabbitMqBasic deadBasic) {
        this.deadBasic = deadBasic;
    }
}
