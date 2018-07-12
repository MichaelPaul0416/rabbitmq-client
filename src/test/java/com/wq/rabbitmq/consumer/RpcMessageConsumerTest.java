package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.BaseRabbitmqTest;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import com.wq.rabbitmq.common.rpc.RpcMessageHandler;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public class RpcMessageConsumerTest extends BaseRabbitmqTest {

    private Set<String> set;

    {
        set = new TreeSet<>();
        set.add("rpc.demo");
    }

    @Override
    protected void doInit() {
        super.messageInvoker = new RpcMessageConsumer("rpc-exchange-direct","rpc-direct-queue",set,"direct");
    }

    @Override
    protected void setExchanger() {
        Channel channel = messageInvoker.getChannel();
        Consumer consumer = new RpcMessageHandler(channel);
        ExchangeConsumer<String> exchangeConsumer = new RpcExchangeConsumer(consumer);

        super.messageInvoker.setExchange(exchangeConsumer);
    }

    @Test
    public void rpcServer(){
        MessageConsumer consumer = (MessageConsumer) messageInvoker;
        ((MessageConsumer) messageInvoker).receiveMessage();
    }
}
