package eu.uk.ncl.di.pet5o.PATH2iot.input;

import eu.uk.ncl.di.pet5o.PATH2iot.network.MessageBus;

import java.util.List;

/**
 * Created by peto on 22/02/2017.
 *
 * Placeholder for the infrastructure to be loaded for operator placement.
 */
public class InfrastructureDesc {
    private List<InfrastructureNode> nodes;
    private MessageBus messageBus;

    public InfrastructureDesc() {}

    public InfrastructureNode getNodeById(int nodeId) {
        for (InfrastructureNode node : nodes) {
            if (node.getNodeId() == nodeId) {
                return node;
            }
        }
        return null;
    }

    public List<InfrastructureNode> getNodes() {
        return nodes;
    }

    public void setNodes(List<InfrastructureNode> nodes) {
        this.nodes = nodes;
    }

    public MessageBus getMessageBus() {
        return messageBus;
    }

    public void setMessageBus(MessageBus messageBus) {
        this.messageBus = messageBus;
    }


}
