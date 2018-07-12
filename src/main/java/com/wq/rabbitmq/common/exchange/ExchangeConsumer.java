package com.wq.rabbitmq.common.exchange;

import com.rabbitmq.client.Channel;
import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.RabbitMqBasic;

import java.util.List;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public interface ExchangeConsumer<T> extends Exchanger {//消费什么类型的数据
    List<MQMessageWrapper<T>> consumerFromServer(Channel channel, RabbitMqBasic rabbitMqBasic);
}
