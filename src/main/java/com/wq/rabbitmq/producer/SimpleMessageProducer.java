package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.wq.rabbitmq.common.*;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import com.wq.rabbitmq.common.exchange.Exchanger;
import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class SimpleMessageProducer implements MessageProducer<String>,ClientLifeCycle {

    private RabbitMqBasic rabbitMqBasic;
    private Connection connection;
    private Channel channel;
    private ExchangeProducer<String> exchangeProducer;

    private static final String CONFIG_PATH = "src/main/resources/rabbitmq.properties";

    public SimpleMessageProducer(String queue, String exchange, Set<String> routingKey, String delivery){
        this.rabbitMqBasic = new RabbitMqBasic();
        rabbitMqBasic.setQueueName(queue);
        rabbitMqBasic.setExchangeName(exchange);
        rabbitMqBasic.setRoutingKey(routingKey);
        rabbitMqBasic.setDelivery(delivery);
        try {
            connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



    @Override
    public void sendMessage(MQMessageWrapper<String> messageWrapper, Set<String> routingKeys) {
        exchangeProducer.bindExchangeAndSend(messageWrapper, routingKeys);
    }



    @Override
    public void initChannel(ConnectionFactoryBuilder.Builder builder) {
        if(connection == null){
            try {
                connection = builder.build();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        try {
            channel = connection.createChannel();
            this.exchangeProducer.prepareEnvironment(channel,rabbitMqBasic);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroyChannel(MessageInvoker messageInvoker) {
        if(messageInvoker.getChannel() != null){
            try {
                messageInvoker.getChannel().close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }

        if(messageInvoker.getConnection() != null){
            try {
                messageInvoker.getConnection().close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getQueueName() {
        return this.rabbitMqBasic.getQueueName();
    }


    private void setExchangeProducer(ExchangeProducer<String> exchangeProducer) {
        this.exchangeProducer = exchangeProducer;
    }

    @Override
    public Connection getConnection() {
        return connection;
    }

    @Override
    public void setExchange(Exchanger exchange) {
        this.setExchangeProducer((ExchangeProducer<String>) exchange);
    }


    @Override
    public Channel getChannel() {
        return channel;
    }

}
