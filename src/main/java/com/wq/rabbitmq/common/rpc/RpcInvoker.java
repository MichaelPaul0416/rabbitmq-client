package com.wq.rabbitmq.common.rpc;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public interface RpcInvoker<P,R> {

    R callRpcMethod(P param);

}
