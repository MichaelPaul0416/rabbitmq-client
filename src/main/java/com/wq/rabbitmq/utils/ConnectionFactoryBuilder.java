package com.wq.rabbitmq.utils;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class ConnectionFactoryBuilder {

    private Properties properties;
    private ConnectionFactory connectionFactory;


    public ConnectionFactoryBuilder(){
        this.connectionFactory = new ConnectionFactory();
    }

    public static class Builder{
        private ConnectionFactoryBuilder factoryBuilder;

        public Builder(Properties properties){
            factoryBuilder = new ConnectionFactoryBuilder();
            factoryBuilder.properties = properties;
            init();
        }

        public Builder(String configPath) throws IOException {
            Properties properties = new Properties();
            properties.load(new FileInputStream(configPath));
            factoryBuilder = new ConnectionFactoryBuilder();
            factoryBuilder.properties = properties;
            init();
        }


        public Connection build() throws IOException, TimeoutException {
            return factoryBuilder.connectionFactory.newConnection();
        }

        private void init(){
            Properties properties = factoryBuilder.properties;
            factoryBuilder.connectionFactory.setUsername(properties.getProperty("username"));
            factoryBuilder.connectionFactory.setPassword(properties.getProperty("password"));
            factoryBuilder.connectionFactory.setVirtualHost(properties.getProperty("virtualHost"));
            factoryBuilder.connectionFactory.setHost(properties.getProperty("host"));
            factoryBuilder.connectionFactory.setPort(Integer.parseInt(properties.getProperty("port")));
        }
    }

}
