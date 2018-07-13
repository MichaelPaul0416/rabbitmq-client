package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.Consumer;
import com.wq.rabbitmq.BaseRabbitmqTest;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import org.junit.Test;
import org.junit.platform.commons.util.StringUtils;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;


/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class SimpleMessageConsumerTest extends BaseRabbitmqTest {


    @Override
    protected void doInit() {
        //切换不同的exchange只需要更改构造器入参即可
//        super.messageInvoker = new SimpleMessageConsumer("rabbitmq-queue-default","","default-routing-key","direct",10);
        //订阅模式
//        super.messageInvoker = new SimpleMessageConsumer("rabbitmq-queue-fanout-1","rabbit-exchange-fanout","",10);

//        super.messageInvoker = new SimpleMessageConsumer("rabbitmq-queue-direct-1","rabbit-exchange-direct","direct-delete",10);


//        ExchangeConsumer<String> exchangeConsumer = new DefaultNullExchangeConsumer(consumer);

//        ((SimpleMessageConsumer) super.messageInvoker).setExchangeConsumer(exchangeConsumer);

        //根据direct从rabbitmq获取消息
//        builderDirectQueues(DIRECT, SEND_SUB_QUEUE_NAME[2]);

//        根据topic获取信息
        builderTopicQueues(TOPIC, "", new String[]{"info.#", "star.*", "*.hello.*","fullName"});
    }


    /**
     *
     * @Author:wangqiang20995
     * @Datetime:2018/7/10 15:22
     * @param: [exchange, queueName--消费者要监听的队列名称, routingKey--路由键集合]
     * @Description:
     *
     **/
    private void builderTopicQueues(String exchange, String queueName, String... routingKey) {

        Set<String> set = new HashSet<>();
        for (String key : routingKey) {
            set.add(key);
        }

        String queue = "";
        if (StringUtils.isNotBlank(queueName)) {
            queue = "rabbitmq-queue-" + exchange + "-" + queueName;
        }
//        super.messageInvoker = new SimpleMessageConsumer(queue, "rabbitmq-exchange-" + exchange,set,10,TOPIC);
        super.messageInvoker = new SimpleMessageConsumer(queue, "rabbitmq-exchange-" + exchange, set, 10, TOPIC,"".equals(queueName));
    }

    private void builderDirectQueues(String exchange, String queueName) {
        Set<String> routingKeys = new TreeSet<>();
        routingKeys.add(exchange + "-" + queueName);
        super.messageInvoker = new SimpleMessageConsumer("rabbitmq-queue-" + exchange + '-' + queueName,
                "rabbit-exchange-" + exchange,
                routingKeys,
                10, DIRECT,false);
    }

    @Override
    protected void setExchanger() {
        Consumer consumer = ((SimpleMessageConsumer) super.messageInvoker).initializedConsumer();
//        direct类型的Exchange
//        ExchangeConsumer<String> exchangeConsumer = new DefaultExchangeConsumer(consumer);
        ExchangeConsumer<String> exchangeConsumer = new TopicExchangeConsumer(consumer);
        super.messageInvoker.setExchange(exchangeConsumer);
    }

    @Test
    public void receiveMessage() {
        ((MessageConsumer) messageInvoker).receiveMessage();
    }

    //多消费者
    @Test
    public void multiConsumer() {
        MessageConsumer<String> consumer = (MessageConsumer<String>) messageInvoker;
        consumer.receiveMessage();
    }

}