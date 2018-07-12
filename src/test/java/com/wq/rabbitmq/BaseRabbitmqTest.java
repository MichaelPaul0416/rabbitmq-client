package com.wq.rabbitmq;

import com.wq.rabbitmq.common.ClientLifeCycle;
import com.wq.rabbitmq.common.invoker.MessageInvoker;
import com.wq.rabbitmq.utils.ConnectionFactoryBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * @Author: wangqiang20995
 * @Date:2018/6/28
 * @Description:
 * @Resource:
 */
public abstract class BaseRabbitmqTest {
    public static final int SEND_MESSAGE_TIMES = 5;

    public static final String[] SEND_SUB_QUEUE_NAME = new String[]{"delete", "update", "insert"};

    public static final String DIRECT = "direct";

    public static final String TOPIC = "topic";

    protected MessageInvoker messageInvoker;

    protected abstract void doInit();

    protected abstract void setExchanger();

    @Before
    public void init() throws IOException {
        doInit();
        setExchanger();
        ClientLifeCycle clientLifeCycle = (ClientLifeCycle) messageInvoker;
        clientLifeCycle.initChannel(new ConnectionFactoryBuilder.Builder("src/main/resources/rabbitmq.properties"));
    }


    @After
    public void destroy(){
        ClientLifeCycle clientLifeCycle = (ClientLifeCycle) messageInvoker;
        clientLifeCycle.destroyChannel(messageInvoker);
    }

}
