package org.apache.rocketmq.mqtt.meta.util;

import org.springframework.context.ApplicationContext;

/**
 * @author dongyuan.pdy
 * date 2022-05-31
 */
public class SpringUtil {

    private static ApplicationContext applicationContext;

    public static void setApplicationContext(ApplicationContext applicationContext) {
        SpringUtil.applicationContext = applicationContext;
    }

    public static <T> T getBeanByClass(Class<T> requiredType) {
        return applicationContext.getBean(requiredType);
    }

    public static Object getBean(String beanName)
    {
        return applicationContext.getBean(beanName);
    }


    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
