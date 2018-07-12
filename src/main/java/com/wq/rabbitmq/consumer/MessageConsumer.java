package com.wq.rabbitmq.consumer;

import com.wq.rabbitmq.common.MQMessageWrapper;
import com.wq.rabbitmq.common.invoker.MessageInvoker;

import java.util.List;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public interface MessageConsumer<T> extends MessageInvoker {

    List<MQMessageWrapper<T>> receiveMessage();
}
