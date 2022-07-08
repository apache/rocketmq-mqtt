package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.Closure;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;

public interface FailoverClosure extends Closure {

    void setResponse(Response response);

    void setThrowable(Throwable throwable);

}
