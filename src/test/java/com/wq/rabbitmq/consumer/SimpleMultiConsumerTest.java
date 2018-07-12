package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.BaseRabbitmqTest;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import org.junit.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class SimpleMultiConsumerTest extends BaseRabbitmqTest {
    @Override
    protected void doInit() {
        Set<String> routingkeys = new TreeSet<>();
        routingkeys.add("");
        super.messageInvoker = new SimpleMessageConsumer("rabbitmq-client-demo","rabbit-client-exchange",routingkeys,10000,DIRECT,false);
    }

    @Override
    protected void setExchanger() {
        Consumer consumer = ((SimpleMessageConsumer) super.messageInvoker).initializedConsumer();
        ExchangeConsumer<String> exchangeConsumer = new DefaultExchangeConsumer(consumer);
        ((SimpleMessageConsumer) super.messageInvoker).setExchange(exchangeConsumer);
    }

    //多消费者
    @Test
    public void multiConsumer(){
        MessageConsumer<String> consumer = (MessageConsumer<String>) messageInvoker;
        consumer.receiveMessage();
    }
}
