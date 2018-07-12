package com.wq.rabbitmq.common;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/29
 * @Description:
 * @Resource:
 */
public enum DeliveryStrategy {

    DEFAULT("direct"),
    DIRECT("direct"),
    FANOUT("fanout"),
    TOPIC("topic");


    public String name;

    DeliveryStrategy(String name){
        this.name = name;
    }

    public static DeliveryStrategy valueof(String strategy){
        DeliveryStrategy[] deliveryStrategies = DeliveryStrategy.values();
        DeliveryStrategy delivery = DEFAULT;
        for (DeliveryStrategy deliveryStrategy : deliveryStrategies){
            if(deliveryStrategy.name.equals(strategy)){
                delivery = deliveryStrategy;
                break;
            }
        }
        return delivery;
    }
}
