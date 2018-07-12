package com.wq.rabbitmq.consumer;

import com.rabbitmq.client.*;
import com.wq.rabbitmq.common.*;
import com.wq.rabbitmq.common.exchange.ExchangeConsumer;
import com.wq.rabbitmq.common.exchange.Exchanger;
import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public class SimpleMessageConsumer implements MessageConsumer<String>, ClientLifeCycle {

    private Channel channel;
    private Connection connection;
    private RabbitMqBasic rabbitMqBasic;
    private ExchangeConsumer<String> exchangeConsumer;
    private boolean chooseStar;

    private int interval;

    private static final String CONFIG_PATH = "src/main/resources/rabbitmq.properties";

    public SimpleMessageConsumer(String queueName, String exchangeName, Set<String> routingKey, int interval, String delivery,boolean chooseStar) {
        this.rabbitMqBasic = new RabbitMqBasic();
        this.rabbitMqBasic.setQueueName(queueName);
        this.rabbitMqBasic.setExchangeName(exchangeName);
        this.rabbitMqBasic.setRoutingKey(routingKey);
        this.rabbitMqBasic.setDelivery(delivery);
        this.interval = interval;
        this.chooseStar = chooseStar;
        try {
            connection = new ConnectionFactoryBuilder.Builder(CONFIG_PATH).build();
            channel = connection.createChannel();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Author:wangqiang20995
     * @Datetime:2018/7/10 14:53
     * @param: [builder]
     * @Description:声明交换机，如果不是topic类型的，再声明指定的队列，然后都要进行绑定
     **/
    @Override
    public void initChannel(ConnectionFactoryBuilder.Builder builder) {


        try {
//            channel = connection.createChannel();

            this.channel.exchangeDeclare(rabbitMqBasic.getExchangeName(), rabbitMqBasic.getDelivery().name, true, true, null);
//            this.channel.exchangeDeclare(rabbitMqBasic.getExchangeName(), rabbitMqBasic.getDelivery().name);
            String queueName = null, starName = null;

            /**
             * 声明队列(如果你已经明确的知道有这个队列,那么下面这句代码可以注释掉,如果不注释掉的话,也可以理解为消费者必须监听一个队列,如果没有就创建一个)
             */
            System.out.println("actually exchange type --> " + this.rabbitMqBasic.getDelivery().name);
            if (!DeliveryStrategy.TOPIC.equals(this.rabbitMqBasic.getDelivery())) {//如果不是topic类型的交换机的话，那么就需要手动声明一个队列
                queueName = channel.queueDeclare(this.rabbitMqBasic.getQueueName(), true, false, false, null).getQueue();
            } else {
                //监听一个随意的队列
                Set<String> keys = this.rabbitMqBasic.getRoutingKey();
                for (String key : keys) {
                    if (key.contains("star") && starName == null) {//包含通配符*
                        starName = channel.queueDeclare().getQueue();
                    } else if(queueName == null){
                        queueName = channel.queueDeclare().getQueue();
                    }
                }
            }
            channel.basicQos(1);//能者多劳模式开启

            //binding
            /**
             * 绑定队列到交换机(这个交换机的名称一定要和上面的生产者交换机名称相同)
             * 参数1：队列的名称
             * 参数2：交换机的名称
             * 参数3：Routing Key
             *
             */
            Set<String> routingKeys = this.rabbitMqBasic.getRoutingKey();
            for (String key : routingKeys) {//后期这里可以扩展为根据路由键指定映射规则【指定一个interface】，并且提供参数给外部接口调用方，比如外部传入1，那么就去第一个if得到的queueName
                if(key.contains("star")){
                    channel.queueBind(starName,this.rabbitMqBasic.getExchangeName(),key,null);
                }else {
                    channel.queueBind(queueName, this.rabbitMqBasic.getExchangeName(), key, null);
                }
            }

            //如果交换机类型是topic并且传入的queueName为空，则在此动态更改队列名
            if(DeliveryStrategy.TOPIC.equals(this.rabbitMqBasic.getDelivery()) && "".equals(this.rabbitMqBasic.getQueueName())){
                if(chooseStar){
                    this.rabbitMqBasic.setQueueName(starName);
                }else {
                    this.rabbitMqBasic.setQueueName(queueName);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroyChannel(MessageInvoker messageInvoker) {
        Channel channel = messageInvoker.getChannel();
        Connection connection = messageInvoker.getConnection();

        if (channel != null) {
            try {
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public List<MQMessageWrapper<String>> receiveMessage() {
//        下面两行方法委托给实际的exchangeConsumer去执行
//        simpleMessageStrategy();
//        exchangeStrategy();
        List<MQMessageWrapper<String>> result = exchangeConsumer.consumerFromServer(channel, rabbitMqBasic);
        while (true) {

        }
    }


    @Override
    public Channel getChannel() {
        return this.channel;
    }

    @Override
    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public void setExchange(Exchanger exchange) {
        this.exchangeConsumer = (ExchangeConsumer<String>) exchange;
    }

    public ExchangeConsumer<String> getExchangeConsumer() {
        return exchangeConsumer;
    }


    class CustomDefaultConsumer<M> extends DefaultConsumer {

        private List<MQMessageWrapper<M>> list;

        public CustomDefaultConsumer(Channel channel) {
            super(channel);
            list = new ArrayList<>();
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            String routingKey = envelope.getRoutingKey();
            String contentType = properties.getContentType();
            long deliveryTag = envelope.getDeliveryTag();

            MQMessageWrapper<M> wrapper = new MQMessageWrapper<>();
            String message = wrapper.deSerialByte(body);
            wrapper.setMessageBody((M) message);
            wrapper.setDateTime(new Date());
            list.add(wrapper);

            getChannel().basicAck(deliveryTag, false);//发送确认消息
            System.out.println("receiveMessage-->" + wrapper);
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public List<MQMessageWrapper<M>> currentReceiveMessage() {
            return this.list;
        }
    }

    public Consumer initializedConsumer() {
        return new CustomDefaultConsumer<String>(this.channel);
    }

}
