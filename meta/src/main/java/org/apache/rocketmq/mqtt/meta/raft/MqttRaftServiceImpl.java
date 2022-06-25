package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.SerializerManager;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rhea.StoreEngineHelper;
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions;
import com.alipay.sofa.jraft.util.BytesUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

public class MqttRaftServiceImpl implements MqttRaftService {
    private static final Logger LOG = LoggerFactory.getLogger(MqttRaftServiceImpl.class);

    private final MqttRaftServer mqttRaftServer;
    private final Executor readIndexExecutor;

    public MqttRaftServiceImpl(MqttRaftServer mqttRaftServer) {
        this.mqttRaftServer = mqttRaftServer;
        this.readIndexExecutor = createReadIndexExecutor();
    }

    private Executor createReadIndexExecutor() {
        final StoreEngineOptions opts = new StoreEngineOptions();
        return StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads());
    }

    @Override
    public void get(final boolean readOnlySafe, final MqttClosure closure) {
        if(!readOnlySafe){
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }

        this.mqttRaftServer.getNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if(status.isOk()){
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }
                MqttRaftServiceImpl.this.readIndexExecutor.execute(() -> {
                    if(isLeader()){
                        LOG.debug("Fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                        applyOperation(CounterOperation.createGet(), closure);
                    }else {
                        handlerNotLeaderError(closure);
                    }
                });
            }
        });
    }

    private boolean isLeader() {
        return this.mqttRaftServer.getFsm().isLeader();
    }

    private long getValue() {
        return this.mqttRaftServer.getFsm().getValue();
    }

    private String getRedirect() {
        return this.mqttRaftServer.redirect().getRedirect();
    }

    @Override
    public void incrementAndGet(final long delta, final MqttClosure closure) {
        applyOperation(CounterOperation.createIncrement(delta), closure);
    }

    private void applyOperation(final CounterOperation op, final MqttClosure closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        try {
            closure.setCounterOperation(op);
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(op)));
            task.setDone(closure);
            this.mqttRaftServer.getNode().apply(task);
        } catch (CodecException e) {
            String errorMsg = "Fail to encode CounterOperation";
            LOG.error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    private void handlerNotLeaderError(final MqttClosure closure) {
        closure.failure("Not leader.", getRedirect());
        closure.run(new Status(RaftError.EPERM, "Not leader"));
    }
}
