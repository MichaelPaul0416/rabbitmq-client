package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;

import java.io.IOException;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public class DefaultNullExchangeProducer implements ExchangeProducer<String> {

    private Channel channel;
    private RabbitMqBasic rabbitMqBasic;

    public DefaultNullExchangeProducer(){}

    @Override
    public void bindExchangeAndSend(MQMessageWrapper<String> wrapper, Set<String> routingKeys) {////就是发送到exchangeType=direct，并且queueName=routingKey.value的队列上
        try {
            /**
             * 向server发布一条消息
             * 参数1：exchange名字，若为空则使用默认的exchange
             * 参数2：routing key
             * 参数3：其他的属性
             * 参数4：消息体
             * RabbitMQ默认有一个exchange，叫default exchange，它用一个空字符串表示，它是direct exchange类型，
             * 任何发往这个exchange的消息都会被路由到routing key的名字对应的队列上，如果没有对应的队列，则消息会被丢弃
             */
            for(String routingKey : routingKeys) {//向空的交换机的多条队列发送消息
                channel.basicPublish("", routingKey, null, wrapper.serialMessage());//routing key = 指定的routingKey
                System.out.println(String.format("发送消息[%s]", wrapper.getMessageBody()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prepareEnvironment(Channel channel, RabbitMqBasic rabbitMqBasic) {
        this.channel = channel;
        this.rabbitMqBasic = rabbitMqBasic;

        /**
         * 声明（创建）队列
         * 参数1：队列名称
         * 参数2：为true时server重启队列不会消失
         * 参数3：队列是否是独占的，如果为true只能被一个connection使用，其他连接建立时会抛出异常,如果是独占队列的话，那么在这个链接关闭时，该队列也会被删除
         * 参数4：队列不再使用时是否自动删除（没有连接，并且没有未处理的消息)
         * 参数5：建立队列时的其他参数
         */
        if(null == rabbitMqBasic.getExchangeName() || "".equals(rabbitMqBasic.getExchangeName())) {
            /**
             * 其实第一个参数应该是取值queueName的，但是因为交换机指定为空，暨取值默认交换机，此时mq会将消息转给routingkey的名字对应的队列上，暨队列名字=routingkey的值
             */
            try {
                Set<String> routingKeys = this.rabbitMqBasic.getRoutingKey();
                for(String routingKey : routingKeys) {
                    this.channel.queueDeclare(routingKey, true, false, false, null);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
