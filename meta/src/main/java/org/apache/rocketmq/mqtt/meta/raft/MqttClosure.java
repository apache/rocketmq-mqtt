package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.error.RaftError;
import com.google.protobuf.Message;
import org.apache.rocketmq.mqtt.common.model.consistency.Response;

public class MqttClosure implements Closure {
    private Message message;

    private Closure closure;

    private MqttStatus mqttStatus = new MqttStatus();

    public MqttClosure(Message message, Closure closure) {
        this.message = message;
        this.closure = closure;
    }

    @Override
    public void run(Status status) {
        mqttStatus.setStatus(status);
        closure.run(mqttStatus);
        clear();
    }

    private void clear() {
        message = null;
        closure = null;
        mqttStatus = null;
    }

    public void setResponse(Response response) {
        this.mqttStatus.setResponse(response);
    }

    public void setThrowable(Throwable throwable) {
        this.mqttStatus.setThrowable(throwable);
    }

    public Message getMessage() {
        return message;
    }

    // Pass the Throwable inside the state machine to the outer layer

    @SuppressWarnings("PMD.ClassNamingShouldBeCamelRule")
    public static class MqttStatus extends Status {

        private Status status;

        private Response response = null;

        private Throwable throwable = null;

        public void setStatus(Status status) {
            this.status = status;
        }

        @Override
        public void reset() {
            status.reset();
        }

        @Override
        public boolean isOk() {
            return status.isOk();
        }

        @Override
        public int getCode() {
            return status.getCode();
        }

        @Override
        public void setCode(int code) {
            status.setCode(code);
        }

        @Override
        public RaftError getRaftError() {
            return status.getRaftError();
        }

        @Override
        public void setError(int code, String fmt, Object... args) {
            status.setError(code, fmt, args);
        }

        @Override
        public void setError(RaftError error, String fmt, Object... args) {
            status.setError(error, fmt, args);
        }

        @Override
        public String toString() {
            return status.toString();
        }

        @Override
        public Status copy() {
            MqttStatus copy = new MqttStatus();
            copy.status = this.status;
            copy.response = this.response;
            copy.throwable = this.throwable;
            return copy;
        }

        @Override
        public String getErrorMsg() {
            return status.getErrorMsg();
        }

        @Override
        public void setErrorMsg(String errMsg) {
            status.setErrorMsg(errMsg);
        }

        public Response getResponse() {
            return response;
        }

        public void setResponse(Response response) {
            this.response = response;
        }

        public Throwable getThrowable() {
            return throwable;
        }

        public void setThrowable(Throwable throwable) {
            this.throwable = throwable;
        }

    }
}
