package com.wq.rabbitmq.common;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public interface SerialTool<T> {

    byte[] serial(T javabean);

    T deSerial(byte[] data);
}
