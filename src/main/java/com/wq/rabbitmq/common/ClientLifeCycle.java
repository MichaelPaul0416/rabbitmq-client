package com.wq.rabbitmq.common;

import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public interface ClientLifeCycle {

    void initChannel(ConnectionFactoryBuilder.Builder builder);

    void destroyChannel(MessageInvoker messageInvoker);
}
