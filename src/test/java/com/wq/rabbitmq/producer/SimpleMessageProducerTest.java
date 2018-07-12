package com.wq.rabbitmq.producer;


import com.wq.rabbitmq.BaseRabbitmqTest;
import com.wq.rabbitmq.common.MQMessageWrapper;
import org.junit.Test;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class SimpleMessageProducerTest extends BaseRabbitmqTest {
    Set<String> set = new HashSet<>();

    {
//        set.add("star.rabbitmq");
//        set.add("info.ok.world");
//        set.add("fuck.hello.world");
        set.add("fullName");
    }


    @Override
    protected void doInit() {
        //空的交换机，消息发送到队列名为routingKey的值的队列上
        super.messageInvoker = new SimpleMessageProducer("rabbitmq-queue-default","",set,"");
        super.messageInvoker.setExchange(new DefaultNullExchangeProducer());

        //非空交换机，发送到指定队列名称上
//        super.messageInvoker = new SimpleMessageProducer("rabbitmq-queue-fanout-3","rabbit-exchange-fanout","","fanout");//订阅模式

        //遍历数组，分别创建不同队列
//        builderDirectQueues("direct", SEND_SUB_QUEUE_NAME[0]);//为三种队列分别创建5个队列


//        builderTopicQueues("topic", SEND_SUB_QUEUE_NAME[0], set);//使用topic的exchange，队列名为rabbitmq-queue-topic-delete,绑定两个routingkey

    }

    private void builderTopicQueues(String exchange, String queueName, Set<String> routingKeys) {
        messageInvoker = new SimpleMessageProducer("rabbitmq-queue-" + exchange + "-" + queueName,
                "rabbitmq-exchange-" + exchange, routingKeys, exchange);
    }

    private void builderDirectQueues(String exchangeType, String queueName) {
        Set<String> routingKeys = new HashSet<>();
        routingKeys.add(exchangeType + "-" + queueName);
        builderTopicQueues(exchangeType, queueName, routingKeys);
    }

    @Override
    protected void setExchanger() {
//        messageInvoker.setExchange(new DefaultExchangeProducer());//exchange is not null
        messageInvoker.setExchange(new DefaultNullExchangeProducer());//use default exchange --> routing message to the queue which name is the value of routing key
    }



    @Test
    public void sendEmptyExchange(){
        MQMessageWrapper<String> wrapper = new MQMessageWrapper<>();
        wrapper.setMessageBody("empty exchange");
        ((MessageProducer)messageInvoker).sendMessage(wrapper,set);
    }

    @Test
    public void sendMessage() {
        MQMessageWrapper<String> messageWrapper = new MQMessageWrapper<String>();
        messageWrapper.setMessageBody("RabbitMQ-rabbitmq-queue-direct-3");
        Set<String> routingKeys = new TreeSet<>();
        routingKeys.add("direct-delete");
        ((MessageProducer) messageInvoker).sendMessage(messageWrapper, routingKeys);
    }

    /**
     * @Author:wangqiang20995
     * @Datetime:2018/6/28 15:57
     * @param: []
     * @Description:一个生产者，多个消费者模式
     **/
    @Test
    public void oneProducer() throws InterruptedException {
        sendMessageWithClient(DIRECT, SEND_MESSAGE_TIMES, 10, "who");

    }

    private void sendMessageWithClient(String exchangeType, int times, int interval, String queueName) throws InterruptedException {
        MessageProducer<String> producer = (MessageProducer<String>) messageInvoker;
        for (int i = 0; i < times; i++) {
            MQMessageWrapper<String> wrapper = new MQMessageWrapper<>();
            wrapper.setMessageBody("RabbitMQ-queue-" + exchangeType + "-" + queueName + (i + 1));
            producer.sendMessage(wrapper, set);
            Thread.sleep(interval);
        }

    }

    @Test
    public void produceTopicMessage() throws InterruptedException {
        MessageProducer<String> producer = (MessageProducer<String>) messageInvoker;
        String message;
        MQMessageWrapper<String> wrapper = new MQMessageWrapper<>();
        int random = new Random().nextInt();
        message = "RabbitMq-queue-topic-delete-" + random;
        wrapper.setMessageBody(message);
        producer.sendMessage(wrapper, set);
        Thread.sleep(10);
    }

}