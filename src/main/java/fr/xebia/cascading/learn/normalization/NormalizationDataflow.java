package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.*;
import cascading.pipe.assembly.Retain;
import cascading.pipe.assembly.Unique;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;


public class NormalizationDataflow {

    public static FlowDef normalize(Tap<?, ?, ?> simmonsSource, Tap<?, ?, ?> nodesSink, Tap<?, ?, ?> nodesRelationsSink) {
        Pipe nodesPipe = new Each("nodesPipe", Fields.ALL, new NodesSplitFunction<>(new Fields("group", "node_type", "node_number", "node_name")), Fields.RESULTS);
        ExpressionFilter nodeNameFilter = new ExpressionFilter("node_name.toLowerCase().contains(\"null\")", String.class);
        nodesPipe = new Each(nodesPipe, new Fields("node_name"), nodeNameFilter);
        nodesPipe = new Unique(nodesPipe, new Fields("node_type", "node_number", "node_name"));
        nodesPipe = new GroupBy(nodesPipe, new Fields("group"));
        nodesPipe = new Every(nodesPipe, new Fields("group"), new EnumeratorBuffer(new Fields("id")), Fields.SWAP);

        Pipe nodesRelationsPipe = new Each("nodesRelationsPipe",
                Fields.ALL,
                new NodesRelationsSplitFunction<>(new Fields(
                        "parent_node_type", "parent_node_number", "parent_node_name",
                        "child_node_type", "child_node_number", "child_node_name",
                        "punch_code")),
                Fields.RESULTS);
        Fields joinParentFieldsRelationsPipe = new Fields("parent_node_type", "parent_node_number", "parent_node_name");
        Fields joinFieldsNodesPipe = new Fields("node_type", "node_number", "node_name");
        Fields newFieldsStep1 = new Fields("parent_node_type", "parent_node_number", "parent_node_name", "child_node_type", "child_node_number", "child_node_name", "punch_code",
                "node_type", "node_number", "node_name", "parent_node_id");
        Pipe joinedPipe = new CoGroup(nodesRelationsPipe, joinParentFieldsRelationsPipe, nodesPipe, joinFieldsNodesPipe, newFieldsStep1, new InnerJoin());
        joinedPipe = new Retain(joinedPipe, new Fields("parent_node_id", "child_node_type", "child_node_number", "child_node_name", "punch_code"));

        Fields joinChildFieldsRelationsPipe = new Fields("child_node_type", "child_node_number", "child_node_name");
        Fields newFieldsStep2 = new Fields("parent_node_id", "child_node_type", "child_node_number", "child_node_name", "punch_code",
                "node_type", "node_number", "node_name", "child_node_id");
        joinedPipe = new CoGroup(joinedPipe, joinChildFieldsRelationsPipe, nodesPipe, joinFieldsNodesPipe, newFieldsStep2, new InnerJoin());
        joinedPipe = new Retain(joinedPipe, new Fields("parent_node_id", "child_node_id", "punch_code"));

        return FlowDef.flowDef()//
                .addSource(nodesPipe, simmonsSource) //
                .addSource(nodesRelationsPipe, simmonsSource)
                .addTail(nodesPipe)//
                .addTail(joinedPipe)
                .addSink(nodesPipe, nodesSink)
                .addSink(joinedPipe, nodesRelationsSink);
    }
}
