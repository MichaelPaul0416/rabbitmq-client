package com.wq.rabbitmq.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.BaseRabbitmqTest;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.exchange.ExchangeProducer;
import com.wq.rabbitmq.common.rpc.RpcMessageReceiver;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public class RpcMessageProducerTest extends BaseRabbitmqTest {

    private Set<String> keys ;

    {
        keys = new TreeSet<>();
        keys.add("rpc.demo");
    }
    @Override
    protected void doInit() {
        super.messageInvoker = new RpcMessageProducer("rpc-direct-queue","rpc-exchange-direct",keys,"direct");

    }

    @Override
    protected void setExchanger() {
        Channel channel = messageInvoker.getChannel();
        Consumer consumer = new RpcMessageReceiver(channel);
        ExchangeProducer<String> exchangeProducer = new RpcExchangeProducer(consumer);

        super.messageInvoker.setExchange(exchangeProducer);
    }

    @Test
    public void rpcClient(){
        MessageProducer<String> producer = (MessageProducer<String>) messageInvoker;
        MQMessageWrapper<String> wrapper = new MQMessageWrapper<>();
        wrapper.setMessageBody("HellWorld-RPC");
        producer.sendMessage(wrapper,keys);
    }
}
