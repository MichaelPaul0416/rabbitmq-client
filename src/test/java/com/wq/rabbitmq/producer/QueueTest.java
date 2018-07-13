package com.wq.rabbitmq.producer;

import com.rabbitmq.client.*;
import com.wq.rabbitmq.AbstractAmqpTest;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

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
        channel.queueBind(deadMessageQueue, forwardDeadMessageExchange, "delay.#", null);

        //声明一个延迟队列
        Map<String, Object> delayQueueMap = new HashMap<>();
        delayQueueMap.put(DELAY_QUEUE_TTL, DELAY_QUEUE_TIME);
        delayQueueMap.put(DELAY_QUEUE_MAX_SIZE, MAX_SIZE);
        delayQueueMap.put(FORWARD_DEAD_MESSAGE_EXCHANGE, forwardDeadMessageExchange);

        channel.queueDeclare(delayQueue, true, false, false, delayQueueMap);

        //将延迟队列与exchange-1绑定
        channel.queueBind(delayQueue, delayExchange, "delay.#");


        /**
         *
         * 纠正一个观念，其实发送的时候routingkey只是作为一个附带信息发送过去，exchange会做路由（此时可以指定队列并且绑定或者不做这两步)
         * 如果指定了队列并且绑定的话，那么消息会被路由并且存储到队列中，如果不绑定的话，那么消息就会被丢弃(无论exchange是什么，都会被抛弃，此时exchange不能为空，因为rabbitmq-server有一个默认的类型为direct的exchange，它的名称就是空字符串)
         * 真正在接收消息的时候，consumer通过exchange的类型以及channel.queueBind绑定的时候指定的routingkey来拉取消息，读到对应的队列中来
         * 在进行queueBind，将queue与exchange进行绑定时，如果exchange的type是topic，那么此时的routingKeys可以是带通配符的，在发送消息channel.basicPublish时，才需要指定具体的routingKey
         */
        for (int i = 0; i < 10; i++) {
            String sendMessage = "delay-queue-message-" + (i + 1);
            if (i % 2 == 0) {
                channel.basicPublish(delayExchange, "delay.message.hello", null, sendMessage.getBytes());
            } else {
                channel.basicPublish(delayExchange, "delay.message.where.are.you", null, sendMessage.getBytes());
            }
            logger.info("send message --> {" + sendMessage + "}");
        }

        while (true) {
        }
    }

    @Test
    public void delayConsumer() throws IOException {
//        String name = channel.queueDeclare().getQueue();

        channel.queueBind(deadMessageQueue, forwardDeadMessageExchange, "delay.message.*", null);
        channel.basicConsume(deadMessageQueue, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("print receive message -->" + new String(body, "UTF-8"));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });
        while (true) {

        }
    }

    @Test
    public void consumeDelayQueue() throws IOException {
        /**
         * 如果在producer中已经将该队列进行绑定到了一样的exchange上，现在如果只是改了routingkey再次进行绑定，其实只是在原先的队列上增加一个路由规则
         * 假设原先绑定的routingkey为A.#，现在新增routingkey=A.message.*，并且在producer中发往队列的消息中有routingkey为A.message.*的消息
         * 并且假设queue中总消息为M，routingkey=A.message.*的消息为N，那么在客户端将A.message.*进行绑定到队列并且进行消息获取之后
         * 最后获得消息数目不是N，而是M，暨队列中的全部消息
         * 原理其实就是在consumer中queueBind其实只是为之后的exchange进行消息路由增加一个规则，假如consumer再次绑定的routingkey=B.#，那么之后exchange会将routingKey=A.#和B.#的消息都路由到该队列
         * 所以consumer进行通配符的queueBind其实不是从队列中拉取这个routingkey对应的消息，而是为与exchange绑定的queue新增一个路由规则，之后如果有符合新增规则的消息发来，就会被绑定的queue记录下来
         */
        channel.queueBind("dead-queue-message", "dead-exchange", "delay.all", null);
        channel.basicConsume("dead-queue-message", false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("print delay queue message -->" + new String(body, "UTF-8"));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });
        while (true) {

        }
    }

    /**
     * 下面a，b两个测试方法，验证了猜想
     * a为服务端，但是没有进行队列绑定，b是客户端，生成一个随机队列并且绑定routingkey规则
     * 1.先启动服务端，发现rabbitmq的控制台没有生成新队列，同时producer的新消息也没有被搞进去，所有的消息虽然过了exchange，
     *      但是exchange上没有绑定队列（即便有绑定队列，但是发送消息时附带的routingkey都不符合现有规则），所以消息都被丢弃
     * 2.先启动客户端，客户端会指定一个routingkey的通配符规则，并且声明一个队列，将该队列通过通配符指定的routingkey与exchange进行绑定，
     *      所以此时exchange-queue的绑定关系已经生成，并且有通配符的routingkey。此时再启动服务端a，发送消息，因为发送的消息时附带的routingkey为真正的routingkey，不含通配符
     *      所以exchange会将该routingkey与含有通配符的routingkey进行匹配，匹配到了那就将该消息放到对应的队列中去，然后因为在客服端中已经绑定了一个consumer监听该队列，所以该消息会被消费
     *      注：有一个办法验证上面的猜想，在客户端中设置不要自动回复ack，暨将autoAck设置为false，并且不执行代码channel.basicAck，
     *          然后启动客户端，在启动服务端，根据下面的例子，服务端发送四条消息，但是在客户端监听的队列中，只有一条消息，并且状态是未确认，
     *          说明客户端的consumer已经获取了消息，只有一条证明其他exchange会将其他不符合routingkey（含通配符）的消息丢弃，只留下routingkey符合要求的消息
     */
    @Test
    public void a() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.56.2");
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setPort(AMQP.PROTOCOL.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 声明转发器
        channel.exchangeDeclare("topic_logs", "topic");
        //定义绑定键
        String[] routing_keys = new String[]{"kernal.info", "cron.warning",
                "auth.info", "kernel.critical"};
        for (String routing_key : routing_keys) {
            //发送4条不同绑定键的消息
            String msg = UUID.randomUUID().toString();
            channel.basicPublish("topic_logs", routing_key, null, msg
                    .getBytes());
            System.out.println(" [x] Sent routingKey = " + routing_key + " ,msg = " + msg + ".");
        }

        channel.close();
        connection.close();
    }

    @Test
    public void b() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.56.2");
        factory.setUsername("admin");
        factory.setPassword("admin");
        factory.setPort(AMQP.PROTOCOL.PORT);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 声明转发器
        channel.exchangeDeclare("topic_logs", "topic");
        // 随机生成一个队列
        String queueName = channel.queueDeclare().getQueue();

        //接收所有与kernel相关的消息
        channel.queueBind(queueName, "topic_logs", "kernel.*");

        System.out.println(" [*] Waiting for messages about kernel. To exit press CTRL+C");

        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("print delay queue message -->" + new String(body, "UTF-8") + "[routingkey] --> " + envelope.getRoutingKey());
//                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        });

        while (true) {
        }
    }

}
