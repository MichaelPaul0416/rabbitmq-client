package com.wq.rabbitmq.common.rpc;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.wq.rabbitmq.common.MQMessageWrapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public class RpcMessageReceiver extends DefaultConsumer {

    private String uuid;
    private RpcMessageCallBack callBack;

    private Logger logger = Logger.getLogger(getClass());

    public RpcMessageReceiver(String uuid,Channel channel,RpcMessageCallBack callBack) {
        super(channel);
        this.uuid = uuid;
        this.callBack = callBack;
    }

    public RpcMessageReceiver(Channel channel,RpcMessageCallBack callBack){
        super(channel);
        this.callBack = callBack;
    }

    public RpcMessageReceiver(Channel channel) {
        super(channel);
    }



    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        String uuid = properties.getCorrelationId();
        MQMessageWrapper<String> wrapper = new MQMessageWrapper<>();
        if(uuid.equals(this.uuid)){
            logger.info("uuid matched,receive rpc method return data...");
            String message = new String(body);
            wrapper.setUuid(uuid);
            wrapper.setMessageBody(message);

        }else {
            wrapper.setMessageBody("rpc请求前与当前获取rpc请求结果中的uuid不一致");
        }
        wrapper.setDateTime(new Date());


        Map<String,Object> map = new HashMap<>();
        map.put("wrapper",wrapper);
        map.put("envelope",envelope);
        this.callBack.callBack(map);
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid(){
        return this.uuid;
    }

    public RpcMessageCallBack getRpcMessageCallBack(){
        return this.callBack;
    }

    public void setRpcMessageCallBack(RpcMessageCallBack callBack){
        this.callBack = callBack;
    }
}
