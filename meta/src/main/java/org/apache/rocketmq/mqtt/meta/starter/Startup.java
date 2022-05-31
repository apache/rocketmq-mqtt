package org.apache.rocketmq.mqtt.meta.starter;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.mqtt.meta.util.SpringUtil;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author dongyuan.pdy
 * date 2022-05-31
 */
public class Startup {
    public static void main(String[] args) {
        System.setProperty(ClientLogger.CLIENT_LOG_USESLF4J, "true");

        ClassPathXmlApplicationContext applicationContext = new ClassPathXmlApplicationContext("classpath:meta_spring.xml");
        SpringUtil.setApplicationContext(applicationContext);
        System.out.println("start main ...");
    }
}
