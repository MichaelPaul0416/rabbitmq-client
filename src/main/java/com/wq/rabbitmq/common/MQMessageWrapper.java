package com.wq.rabbitmq.common;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class MQMessageWrapper<T> {

    private SerialTool serialTools;//序列化工具-->可以使用jdk的序列化，或者protobuf等
    private T messageBody;
    private String dateTime;
    private String uuid;
    private static final String REGEX = "yyyyMMdd-HHmmss";

    public MQMessageWrapper(T messageBody){
        this.messageBody = messageBody;
        serialTools = new DefaultJdkSerial();
    }

    public MQMessageWrapper(){
        serialTools = new DefaultJdkSerial();
    }

    @Override
    public String toString() {
        return "MQMessageWrapper{" +
                "serialTools=" + serialTools +
                ", messageBody=" + messageBody +
                ", dateTime='" + dateTime + '\'' +
                ", uuid=" + uuid +
                '}';
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public byte[] serialMessage(){
        return serialTools.serial(this.messageBody);
    }

    public <T> T deSerialByte(byte[] data){
        return (T) serialTools.deSerial(data);
    }

    public T getMessageBody() {
        return messageBody;
    }

    public void setMessageBody(T messageBody) {
        this.messageBody = messageBody;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(Date dateTime) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(REGEX);
        this.dateTime = dateFormat.format(dateTime);
    }
}
