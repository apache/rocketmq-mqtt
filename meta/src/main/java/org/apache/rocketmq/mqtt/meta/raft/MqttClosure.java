package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import org.apache.rocketmq.mqtt.common.model.ValueResponse;

public abstract class MqttClosure implements Closure {
    private ValueResponse valueResponse;
    private CounterOperation counterOperation;

    public void setCounterOperation(CounterOperation counterOperation) {
        this.counterOperation = counterOperation;
    }

    public CounterOperation getCounterOperation() {
        return counterOperation;
    }

    public ValueResponse getValueResponse() {
        return valueResponse;
    }

    public void setValueResponse(ValueResponse valueResponse) {
        this.valueResponse = valueResponse;
    }

    protected void failure(final String errorMsg, final String redirect) {
        final ValueResponse response = new ValueResponse();
        response.setSuccess(false);
        response.setErrorMsg(errorMsg);
        response.setRedirect(redirect);
        setValueResponse(response);
    }

    protected void success(final long value) {
        final ValueResponse response = new ValueResponse();
        response.setValue(value);
        response.setSuccess(true);
        setValueResponse(response);
    }

    public abstract void run(Status status);
}
