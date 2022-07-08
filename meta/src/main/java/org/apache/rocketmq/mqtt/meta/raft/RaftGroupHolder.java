package org.apache.rocketmq.mqtt.meta.raft;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;

public class RaftGroupHolder {

    private RaftGroupService raftGroupService;

    private MqttStateMachine machine;

    private Node node;

    public RaftGroupHolder(RaftGroupService raftGroupService, MqttStateMachine machine, Node node) {
        this.raftGroupService = raftGroupService;
        this.machine = machine;
        this.node = node;
    }

    public RaftGroupService getRaftGroupService() {
        return raftGroupService;
    }

    public void setRaftGroupService(RaftGroupService raftGroupService) {
        this.raftGroupService = raftGroupService;
    }

    public MqttStateMachine getMachine() {
        return machine;
    }

    public void setMachine(MqttStateMachine machine) {
        this.machine = machine;
    }

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }
}