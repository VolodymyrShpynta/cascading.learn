package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import static fr.xebia.cascading.learn.normalization.ColumnsNames.*;
import static java.lang.String.format;


public class NormalizationDataFlow {

    public static FlowDef normalize(Tap<?, ?, ?> simmonsSource, Tap<?, ?, ?> nodesSink, Tap<?, ?, ?> nodesRelationsSink) {
        Pipe nodesPipe = createNodesPipe();
        Pipe nodesRelationsPipe = createNodesRelationsPipe();
        Pipe nodesIdsRelationsPipe = replaceParentAndChildNodesByTheirIds(nodesPipe, nodesRelationsPipe);

        return FlowDef.flowDef()//
                .addSource(nodesPipe, simmonsSource) //
                .addSource(nodesRelationsPipe, simmonsSource)
                .addTail(nodesPipe)//
                .addTail(nodesIdsRelationsPipe)
                .addSink(nodesPipe, nodesSink)
                .addSink(nodesIdsRelationsPipe, nodesRelationsSink);
    }

    private static Pipe replaceParentAndChildNodesByTheirIds(Pipe nodesPipe, Pipe nodesRelationsPipe) {
        Fields parentFieldsToJoinOn = new Fields(PARENT_NODE_TYPE, PARENT_NODE_NUMBER, PARENT_NODE_NAME);
        Fields nodeFieldsToJoinOn = new Fields(NODE_TYPE, NODE_NUMBER, NODE_NAME);
        Fields resultOfJoinParentFields = new Fields(PARENT_NODE_TYPE, PARENT_NODE_NUMBER, PARENT_NODE_NAME, CHILD_NODE_TYPE, CHILD_NODE_NUMBER, CHILD_NODE_NAME, PUNCH_CODE,
                NODE_TYPE, NODE_NUMBER, NODE_NAME, PARENT_NODE_ID);
        Pipe joinedNodesRelationsPipe = new CoGroup(nodesRelationsPipe, parentFieldsToJoinOn, nodesPipe, nodeFieldsToJoinOn, resultOfJoinParentFields, new InnerJoin());
        joinedNodesRelationsPipe = new Retain(joinedNodesRelationsPipe, new Fields(PARENT_NODE_ID, CHILD_NODE_TYPE, CHILD_NODE_NUMBER, CHILD_NODE_NAME, PUNCH_CODE));

        Fields childFieldsToJoinOn = new Fields(CHILD_NODE_TYPE, CHILD_NODE_NUMBER, CHILD_NODE_NAME);
        Fields resultOfJoinChildFields = new Fields(PARENT_NODE_ID, CHILD_NODE_TYPE, CHILD_NODE_NUMBER, CHILD_NODE_NAME, PUNCH_CODE,
                NODE_TYPE, NODE_NUMBER, NODE_NAME, CHILD_NODE_ID);
        joinedNodesRelationsPipe = new CoGroup(joinedNodesRelationsPipe, childFieldsToJoinOn, nodesPipe, nodeFieldsToJoinOn, resultOfJoinChildFields, new InnerJoin());
        joinedNodesRelationsPipe = new Retain(joinedNodesRelationsPipe, new Fields(PARENT_NODE_ID, CHILD_NODE_ID, PUNCH_CODE));
        return joinedNodesRelationsPipe;
    }

    private static Pipe createNodesPipe() {
        Pipe nodesPipe = new Each(
                "nodesPipe",
                Fields.ALL,
                new NodesSplitFunction<>(new Fields(TMP_GROUP, NODE_TYPE, NODE_NUMBER, NODE_NAME)),
                Fields.RESULTS);
        ExpressionFilter discardTupleWithNullNodeNameFilter = new ExpressionFilter(
                format("%s.toLowerCase().contains(\"[null]\")", NODE_NAME),
                String.class);
        nodesPipe = new Each(nodesPipe, new Fields(NODE_NAME), discardTupleWithNullNodeNameFilter);
        nodesPipe = new Unique(nodesPipe, new Fields(NODE_TYPE, NODE_NUMBER, NODE_NAME));
        nodesPipe = new GroupBy(nodesPipe, new Fields(TMP_GROUP));
        nodesPipe = new Every(
                nodesPipe,
                new Fields(TMP_GROUP),
                new EnumeratorBuffer(new Fields(ID)),
                Fields.SWAP);
        return nodesPipe;
    }

    private static Pipe createNodesRelationsPipe() {
        return new Each(
                "nodesRelationsPipe",
                Fields.ALL,
                new NodesRelationsSplitFunction<>(new Fields(
                        PARENT_NODE_TYPE, PARENT_NODE_NUMBER, PARENT_NODE_NAME,
                        CHILD_NODE_TYPE, CHILD_NODE_NUMBER, CHILD_NODE_NAME,
                        PUNCH_CODE)),
                Fields.RESULTS);
    }
}
