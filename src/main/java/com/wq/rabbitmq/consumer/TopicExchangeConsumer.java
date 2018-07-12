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
 * @Date:2018/7/9
 * @Description:
 * @Resource:
 */
public class TopicExchangeConsumer implements ExchangeConsumer<String> {

    private Consumer consumer;

    public TopicExchangeConsumer(Consumer consumer){
        this.consumer = consumer;
    }

    @Override
    public List<MQMessageWrapper<String>> consumerFromServer(Channel channel, RabbitMqBasic rabbitMqBasic) {
        List<MQMessageWrapper<String>> list = new ArrayList<>();
        try{
            System.out.println("listener queue --> " + rabbitMqBasic.getQueueName());
            channel.basicConsume(rabbitMqBasic.getQueueName(),false,consumer);
            list = ((SimpleMessageConsumer.CustomDefaultConsumer) consumer).currentReceiveMessage();
        }catch (IOException e){
            e.printStackTrace();
        }
        return list;
    }
}
