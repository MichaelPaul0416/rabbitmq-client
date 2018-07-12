package com.wq.rabbitmq.producer;

import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.common.MQMessageWrapper;

import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public interface MessageProducer<T> extends MessageInvoker {

    void sendMessage(MQMessageWrapper<T> messageWrapper, Set<String> routingKeys);

}
