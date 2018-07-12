package com.wq.rabbitmq.common;

import java.util.Set;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class RabbitMqBasic {
    private String queueName;
    private String exchangeName;
    private Set<String> routingKey;
    private DeliveryStrategy delivery;

    public Set<String> getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(Set<String> routingKey) {
        this.routingKey = routingKey;
    }

    @Override
    public String toString() {
        return "RabbitMqBasic{" +
                "queueName='" + queueName + '\'' +
                ", exchangeName='" + exchangeName + '\'' +
                ", routingKey=" + routingKey +
                ", delivery=" + delivery +
                '}';
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public void setDelivery(String delivery){
        this.delivery = DeliveryStrategy.valueof(delivery);
        if(this.exchangeName == null || "".equals(this.exchangeName)){
            this.delivery = DeliveryStrategy.DEFAULT;
        }
    }

    public DeliveryStrategy getDelivery(){
        return this.delivery;
    }
}
