package com.wq.rabbitmq;

import com.rabbitmq.client.*;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/10
 * @Description:
 * @Resource:
 */
public class SimpleAMQPTest {//网上找的demo

    private final static String EXCHANGE_NAME = "topic_logs";

    @Test
    public void producer() throws IOException, TimeoutException {
        /**
         * 创建连接连接到MabbitMQ
         */
        ConnectionFactory factory = new ConnectionFactory();
        // 设置MabbitMQ所在主机ip或者主机名
        factory.setHost("192.168.56.2");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin");
        // 创建一个连接
        Connection connection = factory.newConnection();
        // 创建一个频道
        Channel channel = connection.createChannel();
        // 指定转发——广播
        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

//        channel.queueDeclare("topic-temp-queue",true,false,false,null);

        //所有设备和日志级别
        String[] facilities = {"auth", "cron", "kern", "auth.A"};
        String[] severities = {"error", "info", "warning"};

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 3; j++) {
                //每一个设备，每种日志级别发送一条日志消息
                String routingKey = facilities[i] + "." + severities[j % 3];

                // 发送的消息
                String message = " Hello World![service]:" + facilities[i] + "[log]:" + severities[j % 3];

//                channel.queueBind("topic-temp-queue",EXCHANGE_NAME,routingKey);
                //参数1：exchange name
                //参数2：routing key
                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes());
                System.out.println(" [x] Sent [" + routingKey + "] : '" + message + "'");
            }
        }
        // 关闭频道和连接
        channel.close();
        connection.close();

    }

    @Test
    public void consumePart() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.56.2");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin");
        // 打开连接和创建频道，与发送端一样
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, "topic");
        // 声明一个随机队列
        String queueName = channel.queueDeclare().getQueue();
//        String queueName = channel.queueDeclare("topic-temp-queue",true,false,false,null).getQueue();

        String severity="*.error";//只关注核心错误级别的日志，然后记录到文件中去。
        channel.queueBind(queueName, EXCHANGE_NAME, severity);

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        // 创建队列消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                //记录日志到文件：
                System.out.println(message);
            }
        };
        channel.basicConsume(queueName, true, consumer);
        while (true){

        }
    }

}

