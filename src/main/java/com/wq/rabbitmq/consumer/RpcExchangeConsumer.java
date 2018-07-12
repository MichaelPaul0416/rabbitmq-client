package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import com.wq.rabbitmq.common.rpc.RpcInvoker;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public class RpcExchangeConsumer implements ExchangeConsumer<String> {


    private RpcInvoker<String,MQMessageWrapper<String>> rpcInvoker;
    private Consumer consumer;

    private Logger logger = Logger.getLogger(getClass());

    public RpcExchangeConsumer(Consumer consumer){
        this.consumer = consumer;
    }


    @Override
    public List<MQMessageWrapper<String>> consumerFromServer(Channel channel, RabbitMqBasic rabbitMqBasic) {
        try {
            channel.queueDeclare(rabbitMqBasic.getQueueName(),true,false,false,null);
            channel.basicQos(1);//能者多劳
//            channel.basicConsume(rabbitMqBasic.getQueueName(),false,this.consumer);//使用consumer去监听

            prepare(channel,rabbitMqBasic);

            rpcInvoker.callRpcMethod(null);
        } catch (IOException e) {
            logger.error(e.getMessage(),e);
        }
        return null;
    }

    private void prepare(Channel channel,RabbitMqBasic rabbitMqBasic){
        this.rpcInvoker = new DefaultRpcServerInvoker(channel,consumer,rabbitMqBasic);

    }
}
