package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public class DefaultExchangeConsumer implements ExchangeConsumer<String> {

    private Consumer consumer;

    public DefaultExchangeConsumer(Consumer consumer){
        this.consumer = consumer;
    }

    @Override
    public List<MQMessageWrapper<String>> consumerFromServer(Channel channel, RabbitMqBasic rabbitMqBasic) {
        List<MQMessageWrapper<String>> list = new ArrayList<>();
        try {

            channel.basicConsume(rabbitMqBasic.getQueueName(),false,consumer);

            list = ((SimpleMessageConsumer.CustomDefaultConsumer) consumer).currentReceiveMessage();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}
