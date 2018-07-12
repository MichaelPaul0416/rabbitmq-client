package com.wq.rabbitmq.common.rpc;

import com.rabbitmq.client.*;
import com.wq.rabbitmq.common.MQMessageWrapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: wangqiang20995
 * @Date:2018/7/11
 * @Description:
 * @Resource:
 */
public class RpcMessageHandler extends DefaultConsumer {

    private RpcMessageCallBack callBack;

    private Logger logger = Logger.getLogger(getClass());

    public RpcMessageHandler(Channel channel, RpcMessageCallBack callBack) {
        super(channel);
        this.callBack = callBack;
    }

    public RpcMessageHandler(Channel channel) {
        super(channel);
    }

    /**
     * @param consumerTag
     * @param envelope
     * @param properties
     * @param body
     * @throws IOException 接受并且解码mq消息队列中推送过来的消息
     */
    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
//        super.handleDelivery(consumerTag, envelope, properties, body);
        doHandler(envelope, properties, body);

    }


    private void doHandler(Envelope envelope, AMQP.BasicProperties properties, byte[] bytes) {
        Map<String, Object> results = new HashMap<>();
        String uuid = properties.getCorrelationId();
        logger.info("开始处理uuid为[" + uuid + "]的RPC请求");
        String request = new String(bytes);
        String md5Response = MD5(request);

        MQMessageWrapper<byte[]> wrapper = new MQMessageWrapper<>();
        wrapper.setMessageBody(md5Response.getBytes());
        wrapper.setDateTime(new Date());
        wrapper.setUuid(uuid);

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        builder.correlationId(uuid);

        logger.info("callback method with request [" + wrapper + "]");
        results.put("wrapper", wrapper);
        results.put("replyTo", properties.getReplyTo());
        results.put("properties", builder.build());
        results.put("envelope", envelope);

        this.callBack.callBack(results);
    }

    private String MD5(String info) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            System.out.println(e.toString());
            e.printStackTrace();
            return "";
        }
        char[] charArray = info.toCharArray();
        byte[] byteArray = new byte[charArray.length];

        for (int i = 0; i < charArray.length; i++)
            byteArray[i] = (byte) charArray[i];
        byte[] md5Bytes = md5.digest(byteArray);
        StringBuffer hexValue = new StringBuffer();
        for (int i = 0; i < md5Bytes.length; i++) {
            int val = ((int) md5Bytes[i]) & 0xff;
            if (val < 16)
                hexValue.append("0");
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }

    public void setCallBack(RpcMessageCallBack callBack){
        this.callBack = callBack;
    }

    public RpcMessageCallBack getCallBack(){
        return this.callBack;
    }
}
