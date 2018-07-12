package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public class DefaultNullExchangeConsumer implements ExchangeConsumer<String> {

    private Consumer consumer;

    public DefaultNullExchangeConsumer(Consumer consumer){
        this.consumer = consumer;
    }

    @Override
    public List<MQMessageWrapper<String>> consumerFromServer(Channel channel, RabbitMqBasic rabbitMqBasic) {
        List<MQMessageWrapper<String>> list = new ArrayList<>();


        try {

            /**
             * 监听队列
             * 参数1:队列名称
             * 参数2：是否发送ack包，不发送ack消息会持续在服务端保存，直到收到ack。  可以通过channel.basicAck手动回复ack
             * 参数3：消费者
             */
            Set<String> routingKeys = rabbitMqBasic.getRoutingKey();
            for(String routingKey : routingKeys) {//一个信道监听多个队列
                channel.basicConsume(routingKey, false, consumer);
            }

            list = ((SimpleMessageConsumer.CustomDefaultConsumer) consumer).currentReceiveMessage();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }
}
