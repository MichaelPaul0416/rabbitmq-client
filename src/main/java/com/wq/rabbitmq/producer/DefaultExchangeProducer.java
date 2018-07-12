package com.wq.rabbitmq.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.wq.rabbitmq.common.DeliveryStrategy;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

import static com.rabbitmq.client.MessageProperties.PERSISTENT_TEXT_PLAIN;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public class DefaultExchangeProducer implements ExchangeProducer<String> {

    private Channel channel;
    private RabbitMqBasic rabbitMqBasic;
    private boolean persistent;

    private Logger logger = Logger.getLogger(getClass());

    public DefaultExchangeProducer(boolean persistent) {
        this.persistent = persistent;
    }

    @Override
    public void bindExchangeAndSend(MQMessageWrapper<String> wrapper, Set<String> routingKeys) {

        try {

            AMQP.BasicProperties basicProperties = persistent ? PERSISTENT_TEXT_PLAIN : null;
            for (String routingKey : routingKeys) {
                channel.basicPublish(rabbitMqBasic.getExchangeName(), routingKey, basicProperties, wrapper.serialMessage());
                System.out.println("subscriber send [routingKey]-->[" + routingKey + "]--> " + wrapper.getMessageBody());
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void prepareEnvironment(Channel channel, RabbitMqBasic rabbitMqBasic) {
        this.channel = channel;
        this.rabbitMqBasic = rabbitMqBasic;

        //do something if necessary
        try {
            /**
             * 声明exchange(交换机)
             * 参数1：交换机名称
             * 参数2：交换机类型
             * 参数3：交换机持久性，如果为true则服务器重启时不会丢失
             * 参数4：交换机在不被使用时是否删除
             * 参数5：交换机的其他属性
             */

            if ("".equals(this.rabbitMqBasic.getExchangeName())) {
                logger.debug("use default exchange and it's type is direct");
                return;
            }
            channel.exchangeDeclare(this.rabbitMqBasic.getExchangeName(), this.rabbitMqBasic.getDelivery().name, true, true, null);
            System.out.println("producer delivery type --> " + this.rabbitMqBasic.getDelivery().name);
            if (!DeliveryStrategy.TOPIC.equals(rabbitMqBasic.getDelivery())) {
                channel.queueDeclare(this.rabbitMqBasic.getQueueName(), true, false, false, null);//声明一个，不存在就创建
                Set<String> routingKeys = this.rabbitMqBasic.getRoutingKey();
                for (String routingKey : routingKeys) {
                    channel.queueBind(this.rabbitMqBasic.getQueueName(), this.rabbitMqBasic.getExchangeName(), routingKey);
                }
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
