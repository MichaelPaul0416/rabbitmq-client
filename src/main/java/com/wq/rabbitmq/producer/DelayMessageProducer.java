package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.wq.rabbitmq.common.ClientLifeCycle;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.Exchanger;
import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/13
 * @Description:
 * @Resource:
 */
public class DelayMessageProducer implements MessageProducer<String>,ClientLifeCycle {


    private Channel channel;
    private Connection connection;
    private RabbitMqBasic delayBasic;
    private RabbitMqBasic deadBasic;
    private DelayExchangeProducer exchangeProducer;

    private static final String CONFIG_PATH = "src/main/resources/rabbitmq.properties";
    private Logger logger = Logger.getLogger(getClass());

    public DelayMessageProducer(RabbitMqBasic delayBasic,RabbitMqBasic deadBasic){
        this.deadBasic = deadBasic;
        this.delayBasic = delayBasic;
        try {
            connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }

    }

    @Override
    public void initChannel(ConnectionFactoryBuilder.Builder builder) {
        if(this.connection == null){
            logger.warn("init connection with rabbitmq-server failed and will try again");
            try{
                this.connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
            }catch (Exception e){
                logger.error("init connection failed and will exit");
                System.exit(1);
            }
        }

        try {
            this.channel = connection.createChannel();
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }

        this.exchangeProducer.setDeadBasic(deadBasic);
        this.exchangeProducer.setDelayBasic(delayBasic);
        this.exchangeProducer.prepareEnvironment(this.channel,null);
    }

    @Override
    public void destroyChannel(MessageInvoker messageInvoker) {
        if(this.channel != null){
            try{
                channel.close();
            }catch (Exception e){
                logger.error(e.getMessage(),e);
            }
        }

        if(this.connection != null){
            try{
                this.connection.close();
            }catch (Exception e){
                logger.error(e.getMessage(),e);
            }
        }
    }

    @Override
    public void sendMessage(MQMessageWrapper<String> messageWrapper, Set<String> routingKeys) {
        this.exchangeProducer.bindExchangeAndSend(messageWrapper,routingKeys);
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
        this.exchangeProducer = (DelayExchangeProducer) exchange;
    }
}
