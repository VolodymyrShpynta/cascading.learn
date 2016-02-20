package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Unique;
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

        return FlowDef.flowDef()//
                .addSource(nodesPipe, simmonsSource) //
                .addSource(nodesRelationsPipe, simmonsSource)
                .addTail(nodesPipe)//
                .addTail(nodesRelationsPipe)
                .addSink(nodesPipe, nodesSink)
                .addSink(nodesRelationsPipe, nodesRelationsSink);
    }
}
