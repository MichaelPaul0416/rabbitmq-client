package com.wq.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;
import org.apache.log4j.Logger;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/12
 * @Description:
 * @Resource:
 */
public abstract class AbstractAmqpTest {

    protected Connection connection;

    protected Channel channel;

    protected Logger logger = Logger.getLogger(getClass());

    private static final String CONFIG_PATH = "src/test/resources/rabbitmq.properties";
    public AbstractAmqpTest(){
        try {
            connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
            channel = connection.createChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
