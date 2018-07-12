package com.wq.rabbitmq.producer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.wq.rabbitmq.AbstractAmqpTest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/12
 * @Description:
 * @Resource:
 */
public class QueueTest extends AbstractAmqpTest {

    private static final String DELAY_QUEUE_TTL = "x-message-ttl";

    private static final String DELAY_QUEUE_MAX_SIZE = "x-max-length";

    private static final String FORWARD_DEAD_MESSAGE_EXCHANGE = "x-dead-letter-exchange";

    private static final int DELAY_QUEUE_TIME = 20000;//20s

    private static final int MAX_SIZE = 10;

    String delayExchange = "delay-exchange-one";
    String forwardDeadMessageExchange = "forward-dead-message-exchange";
    String delayQueue = "delay-message-queue";
    String deadMessageQueue = "dead-message-receive-queue";

    @Test
    public void delayQueue() throws IOException {
        /**
         * 1.producer发送消息到exchange-1，可以指定routingkey
         * 2.定义一个exchange-2，用于接收dead-message
         * 3.定义一个delay-queue
         * 4.定义一个名为accept-dead-letter的queue，用于接收超时的queue中消息
         * 5.exchange-1根据routingkey将message分发到delay-queue上
         * 6.等待超时之后，delay-queue将dead-message经过exchange-2转发到accept-dead-letter队列
         * 7.consumer监听accept-dead-letter队列，有消息就输出，打到延时队列的目的
         */


        //客户端第一次发送消息到的exchange
        channel.exchangeDeclare(delayExchange, "topic", true, false, null);

        //声明一个路由dead-message的exchange
        channel.exchangeDeclare(forwardDeadMessageExchange, "topic", true, false, null);

        //声明一个接收dead-message的队列
        channel.queueDeclare(deadMessageQueue, true, false, false, null);
        channel.queueBind(deadMessageQueue,forwardDeadMessageExchange,"delay.*",null);

        //声明一个延迟队列
        Map<String, Object> delayQueueMap = new HashMap<>();
        delayQueueMap.put(DELAY_QUEUE_TTL, DELAY_QUEUE_TIME);
        delayQueueMap.put(DELAY_QUEUE_MAX_SIZE, MAX_SIZE);
        delayQueueMap.put(FORWARD_DEAD_MESSAGE_EXCHANGE, forwardDeadMessageExchange);

        channel.queueDeclare(delayQueue, true, false, false, delayQueueMap);

        //将延迟队列与exchange-1绑定
        channel.queueBind(delayQueue, delayExchange, "delay.message");


        /**
         *
         * 纠正一个观念，其实发送的时候routingkey只是作为一个附带信息发送过去，exchange会做路由（此时可以指定队列并且绑定或者不做这两步)
         * 如果指定了队列并且绑定的话，那么消息会被路由并且存储到队列中，如果不绑定的话，那么消息就会被丢弃(无论exchange是什么，都会被抛弃，此时exchange不能为空，因为rabbitmq-server有一个默认的类型为direct的exchange，它的名称就是空字符串)
         * 真正在接收消息的时候，consumer通过exchange的类型以及channel.queueBind绑定的时候指定的routingkey来拉取消息，读到对应的队列中来
         */
        for (int i = 0; i < 10; i++) {
            String sendMessage = "delay-queue-message-" + (i + 1);
            channel.basicPublish(delayExchange, "delay.message", null, sendMessage.getBytes());
            logger.info("send message --> {" + sendMessage + "}");
        }

        while (true) {
        }
    }

    @Test
    public void delayConsumer() throws IOException {
//        String name = channel.queueDeclare().getQueue();

        channel.queueBind(deadMessageQueue, delayExchange, "delay.*", null);
        channel.basicConsume(deadMessageQueue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("print receive message -->" + new String(body));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });
        while (true) {

        }
    }
}
