package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.wq.rabbitmq.common.ClientLifeCycle;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import com.wq.rabbitmq.common.exchange.Exchanger;
import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public class RpcMessageConsumer implements MessageConsumer<String>,ClientLifeCycle {

    private Channel channel;
    private Connection connection;
    private RabbitMqBasic rabbitMqBasic;
    private ExchangeConsumer<String> exchangeConsumer;

    private Logger logger = Logger.getLogger(getClass());
    private static final String CONFIG_PATH = "src/main/resources/rabbitmq.properties";

    public RpcMessageConsumer(String exchange, String queue, Set<String> keys,String delivery){
        this.rabbitMqBasic = new RabbitMqBasic();
        this.rabbitMqBasic.setDelivery(delivery);
        this.rabbitMqBasic.setRoutingKey(keys);
        this.rabbitMqBasic.setExchangeName(exchange);
        this.rabbitMqBasic.setQueueName(queue);

        try {
            this.connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void initChannel(ConnectionFactoryBuilder.Builder builder) {
        if(this.connection ==  null){
            logger.warn("connected with rabbitmq-server failed and will try one more");
            try {
                this.connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
        }

        try {
            this.channel = connection.createChannel();
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
    }

    @Override
    public void destroyChannel(MessageInvoker messageInvoker) {
        if(this.channel != null){
            try {
                channel.close();
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
        }

        if(this.connection != null){
            try {
                connection.close();
            } catch (IOException e) {
                logger.error(e.getMessage(),e);
            }
        }
    }

    @Override
    public List<MQMessageWrapper<String>> receiveMessage() {
        return exchangeConsumer.consumerFromServer(this.channel,this.rabbitMqBasic);
    }

    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public void setExchange(Exchanger exchange) {
        this.exchangeConsumer = (ExchangeConsumer<String>) exchange;
    }
}
