package com.wq.rabbitmq.common.exchange;

import com.rabbitmq.client.Channel;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;

import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public interface ExchangeProducer<T> extends Exchanger {

    void bindExchangeAndSend(MQMessageWrapper<T> wrapper, Set<String> routingKeys);

    void prepareEnvironment(Channel channel,RabbitMqBasic rabbitMqBasic);
}
