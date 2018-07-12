package com.wq.rabbitmq.common.invoker;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.wq.rabbitmq.common.exchange.Exchanger;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public interface MessageInvoker {

    Channel getChannel();

    Connection getConnection();

    void setExchange(Exchanger exchange);
}
