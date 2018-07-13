package com.wq.rabbitmq.producer;

import com.wq.rabbitmq.BaseRabbitmqTest;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import org.junit.Test;

import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/13
 * @Description:
 * @Resource:
 */
public class DelayQueueProducerTest extends BaseRabbitmqTest {

    private Set<String> routingKeys;//发送的时候可以理解为exchange需要具体的routingkey根据binding时指定的规则进行路由

    private Set<String> bindingKeys;//binding的时候可以理解为只是指定一个规则

    {
        routingKeys = new TreeSet<>();
        routingKeys.add("delay.queue.demo");
        routingKeys.add("delay.hello.world");
        routingKeys.add("delay.all");

        bindingKeys = new TreeSet<>();
        bindingKeys.add("delay.#");
    }

    @Override
    protected void doInit() {
        RabbitMqBasic delayBasic = new RabbitMqBasic();
        delayBasic.setExchangeName("delay-exchange");
        delayBasic.setQueueName("delay-queue-message");
        delayBasic.setRoutingKey(bindingKeys);
        delayBasic.setDelivery("topic");

        RabbitMqBasic deadBasic = new RabbitMqBasic();
        deadBasic.setExchangeName("dead-exchange");
        deadBasic.setQueueName("dead-queue-message");
        deadBasic.setRoutingKey(bindingKeys);
        deadBasic.setDelivery("topic");

        super.messageInvoker = new DelayMessageProducer(delayBasic, deadBasic);
    }

    @Override
    protected void setExchanger() {
        DelayExchangeProducer delayExchangeProducer = new DelayExchangeProducer(1000 * 10, 1000);
        super.messageInvoker.setExchange(delayExchangeProducer);
    }

    @Test
    public void delayQueue() {
        MQMessageWrapper<String> wrapper = new MQMessageWrapper<>();
        MessageProducer<String> producer = (MessageProducer<String>) super.messageInvoker;
        for (int i = 0; i < 90; i++) {
            String message = "delay queue -- message [" + (i + 1) + "]";
            wrapper.setMessageBody(message);
            wrapper.setDateTime(new Date());
            producer.sendMessage(wrapper,routingKeys);
        }
    }
}
