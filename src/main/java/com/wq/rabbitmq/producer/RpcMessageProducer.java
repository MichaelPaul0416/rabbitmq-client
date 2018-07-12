package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.wq.rabbitmq.common.ClientLifeCycle;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import com.wq.rabbitmq.common.exchange.Exchanger;
import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/10
 * @Description:RPC中请求的产生方，暨RPCClient
 * @Resource:
 */
public class RpcMessageProducer implements MessageProducer<String>,ClientLifeCycle {

    private Channel channel;
    private Connection connection;
    private ExchangeProducer<String> exchanger;
    private RabbitMqBasic rabbitMqBasic;

    private static final String CONFIG_LOCATION = "src/main/resources/rabbitmq.properties";
    private Logger logger = Logger.getLogger(getClass());

//    queue为rpc接受消息的队列，暨需要监听rpc服务端计算完消息之后，存储计算结果的消息队列
    public RpcMessageProducer(String listenerQueue,String exchange,Set<String> keys,String delivery){
        this.rabbitMqBasic = new RabbitMqBasic();
        rabbitMqBasic.setQueueName(listenerQueue);
        rabbitMqBasic.setExchangeName(exchange);
        rabbitMqBasic.setRoutingKey(keys);
        rabbitMqBasic.setDelivery(delivery);

        try {
            this.connection = new ConnectionFactoryBuilder.Builder(CONFIG_LOCATION).build();
        }catch (Exception e){
            logger.error(e.getMessage(),e);
        }


    }
    @Override
    public void initChannel(ConnectionFactoryBuilder.Builder builder) {
        if(this.connection == null){
            logger.warn("connection between rabbitmq-server and local build failed at first time, and will try again");
            try {
                this.connection = new ConnectionFactoryBuilder.Builder(CONFIG_LOCATION).build();
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
        }

        try {
            this.channel = this.connection.createChannel();
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }

        this.exchanger.prepareEnvironment(this.channel,this.rabbitMqBasic);//做一些exchange的预处理
    }

    @Override
    public void destroyChannel(MessageInvoker messageInvoker) {

        if(this.channel != null){
            try {
                this.channel.close();
            } catch (Exception e) {
                logger.error(e.getMessage(),e);
            }
        }

        if(this.connection != null){
            try {
                this.connection.close();
            } catch (IOException e) {
                logger.error(e.getMessage(),e);
            }
        }
    }

    @Override
    public void sendMessage(MQMessageWrapper<String> messageWrapper, Set<String> routingKeys) {
        logger.info("prepare call exchange producer to send message");
        this.exchanger.bindExchangeAndSend(messageWrapper,routingKeys);
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
        this.exchanger = (ExchangeProducer<String>) exchange;
    }


}
