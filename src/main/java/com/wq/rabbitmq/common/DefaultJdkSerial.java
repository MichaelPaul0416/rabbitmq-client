package com.wq.rabbitmq.common;

import java.io.*;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class DefaultJdkSerial implements SerialTool {

    private ByteArrayInputStream inputStream;
    private ByteArrayOutputStream outputStream;


    public byte[] serial(Object javabean) {
        outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = null;
        byte[] bytes = null;
        try {
            objectOutputStream = new ObjectOutputStream(outputStream);
            objectOutputStream.writeObject(javabean);
            objectOutputStream.flush();
            bytes = outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(objectOutputStream != null){
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(outputStream != null){
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return bytes;
    }

    public Object deSerial(byte[] data) {
        inputStream = new ByteArrayInputStream(data);
        ObjectInputStream objectInputStream = null;
        Object object = null;
        try {
            objectInputStream = new ObjectInputStream(inputStream);
            object = objectInputStream.readObject();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }finally {
            if(objectInputStream != null){
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if(inputStream != null){
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return object;
    }
}
