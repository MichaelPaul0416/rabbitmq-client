package com.wq.rabbitmq.producer;

import com.rabbitmq.client.*;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.rpc.RpcInvoker;
import com.wq.rabbitmq.common.rpc.RpcMessageCallBack;
import com.wq.rabbitmq.common.rpc.RpcMessageReceiver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:入参为String类型，出参希望得到封装类型
 * @Resource:
 */
public class DefaultRpcClientInvoker implements RpcInvoker<String, MQMessageWrapper<String>>, RpcMessageCallBack {


    private Channel channel;
    private Consumer consumer;
    private RabbitMqBasic rabbitMqBasic;
    private boolean shutdown;
    private Map<String, MQMessageWrapper<String>> rpcResults;
    private String listenerQueue;

    private Logger logger = Logger.getLogger(getClass());

    public DefaultRpcClientInvoker(Channel channel, Consumer consumer, String listenerQueue, RabbitMqBasic rabbitMqBasic) {
        this.channel = channel;
        this.consumer = consumer;
        this.rabbitMqBasic = rabbitMqBasic;
        this.rpcResults = new ConcurrentHashMap<>();
        this.listenerQueue = listenerQueue;
    }

    @Override
    public MQMessageWrapper<String> callRpcMethod(String param) {


        logger.debug("开始调用RPC方法");
        String uuid = UUID.randomUUID().toString();

        doCheck(uuid);

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties().builder();
        builder.correlationId(uuid);
        builder.replyTo(this.listenerQueue);//rpc处理结果消息接受队列

        AMQP.BasicProperties basicProperties = builder.build();
        logger.debug("发送消息给RabbitMQ-Server[" + param +"]");
        try {
            for (String key : this.rabbitMqBasic.getRoutingKey()) {
                this.channel.basicPublish(this.rabbitMqBasic.getExchangeName(), key, basicProperties, param.getBytes());
            }
            channel.basicConsume(this.listenerQueue, false, consumer);
        } catch (IOException e) {
            logger.error("发送消息失败：" + e.getMessage(), e);
            shutdown = true;
        }
        while (!shutdown) {

        }

        return this.rpcResults.get(uuid);
    }

    private void doCheck(String uuid) {
        String id = ((RpcMessageReceiver) consumer).getUuid();
        if (id == null || "".equals(id)) {
            ((RpcMessageReceiver) consumer).setUuid(uuid);
        }

        RpcMessageCallBack callBack = ((RpcMessageReceiver) consumer).getRpcMessageCallBack();
        if (callBack == null) {
            ((RpcMessageReceiver) consumer).setRpcMessageCallBack(this);
        }
    }

    @Override
    public void callBack(Object result) {
        Map<String, Object> map = (Map<String, Object>) result;
        MQMessageWrapper<String> messageWrapper = (MQMessageWrapper<String>) map.get("wrapper");
        this.rpcResults.put(messageWrapper.getUuid(), messageWrapper);
        logger.info("得到uuid为[" + messageWrapper.getUuid() + "]的RPC计算结果[" + messageWrapper.getMessageBody() + "]");
        Envelope envelope = (Envelope) map.get("envelop");
        try {
            this.channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        //为了方便，更改的条件可以更加复杂一点
        this.shutdown = true;
    }
}
