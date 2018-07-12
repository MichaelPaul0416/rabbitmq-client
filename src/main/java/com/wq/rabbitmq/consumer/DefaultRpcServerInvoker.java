package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;
import com.wq.rabbitmq.common.rpc.RpcInvoker;
import com.wq.rabbitmq.common.rpc.RpcMessageCallBack;
import com.wq.rabbitmq.common.rpc.RpcMessageHandler;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:Rpc请求的服务端处理者
 * @Resource:
 */
public class DefaultRpcServerInvoker implements RpcInvoker<String, MQMessageWrapper<String>>, RpcMessageCallBack {

    private Channel channel;
    private Consumer consumer;//消息接受处理器
    private RabbitMqBasic rabbitMqBasic;


    private AtomicBoolean complete;//处理一个rpc请求完毕的标志

    private Logger logger = Logger.getLogger(getClass());

    public DefaultRpcServerInvoker(Channel channel, Consumer consumer, RabbitMqBasic rabbitMqBasic) {
        this.channel = channel;
        this.consumer = consumer;
        this.rabbitMqBasic = rabbitMqBasic;
        complete = new AtomicBoolean(false);
    }

    @Override
    public MQMessageWrapper<String> callRpcMethod(String param) {
        logger.info("接受RPC请求，开始处理请求");

        check();

        try {
            this.channel.basicConsume(this.rabbitMqBasic.getQueueName(), false, consumer);//从消息生产者的队列中拉取消息

        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            this.complete.compareAndSet(false, true);//出现异常，不再接受请求
        }
        while (!complete.get()) {

        }
        return null;
    }

    private void check() {
        RpcMessageHandler handler = (RpcMessageHandler) consumer;
        if(handler.getCallBack() == null){
            handler.setCallBack(this);
        }
    }

    @Override
    public void callBack(Object result) {
        Map<String, Object> map = (Map<String, Object>) result;

//        模拟只能接受一次请求，如果需要一直接受请求的话，那么属性值不需要改变就可以
//        if(response.getMessageBody() != null ){
//            此时应该是没处理完
//            complete.compareAndSet(false,true);
//        }
        String replyTo = (String) map.get("replyTo");
        AMQP.BasicProperties properties = (AMQP.BasicProperties) map.get("properties");
        MQMessageWrapper<byte[]> wrapper = (MQMessageWrapper<byte[]>) map.get("wrapper");
        Envelope envelope = (Envelope) map.get("envelope");

        try {
//            this.channel.basicPublish(this.rabbitMqBasic.getExchangeName(), replyTo, properties, wrapper.getMessageBody());
            this.channel.basicPublish("", replyTo, properties, wrapper.getMessageBody());
            this.channel.basicAck(envelope.getDeliveryTag(), false);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            this.complete.compareAndSet(false, true);
        }
    }
}
