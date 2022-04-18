package org.apache.rocketmq.mqtt.common.util;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringUtils {
    private static ClassPathXmlApplicationContext applicationContext;

    public static void SetClassPathXmlApplicationContext(ClassPathXmlApplicationContext _applicationContext) {
        applicationContext = _applicationContext;
    }

    public static <T> T getBean(Class<T> type) {
        if (applicationContext == null) {
            return null;
        }
        return applicationContext.getBean(type);
    }
}
