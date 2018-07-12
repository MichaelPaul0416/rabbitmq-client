package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import com.wq.rabbitmq.common.rpc.RpcInvoker;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/10
 * @Description:
 * @Resource:
 */
public class RpcExchangeProducer implements ExchangeProducer<String> {

    private RpcInvoker<String, MQMessageWrapper<String>> rpcInvoker;
    private Logger logger = Logger.getLogger(getClass());
    private Consumer consumer;

    public RpcExchangeProducer(Consumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public void bindExchangeAndSend(MQMessageWrapper<String> wrapper, Set<String> routingKeys) {
        logger.debug("rpc client produce a message and will send to server");
        MQMessageWrapper<String> result = rpcInvoker.callRpcMethod(wrapper.getMessageBody());
        logger.info("RPC result --> " + result);
    }

    @Override
    public void prepareEnvironment(Channel channel, RabbitMqBasic rabbitMqBasic) {


        //register a callback queue
        try {
            channel.exchangeDeclare(rabbitMqBasic.getExchangeName(), rabbitMqBasic.getDelivery().name);
//            声明rpc消息发送队列
            channel.queueDeclare(rabbitMqBasic.getQueueName(), true, false, false, null);
            for (String key : rabbitMqBasic.getRoutingKey()) {
                channel.queueBind(rabbitMqBasic.getQueueName(), rabbitMqBasic.getExchangeName(), key);
            }
//            声明rpc消息接受服务端返回消息的队列,这个接受rpc处理完毕的消息的队列也可以是指定的消息队列，但是此处为了方便，使用随机生成的第一个队列
            String queue = channel.queueDeclare().getQueue();
            logger.info("listen queue --> " + queue);

            this.rpcInvoker = new DefaultRpcClientInvoker(channel, this.consumer, queue, rabbitMqBasic);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
