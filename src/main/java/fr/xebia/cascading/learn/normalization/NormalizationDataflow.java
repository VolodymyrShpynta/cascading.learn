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

    public static FlowDef normalize(Tap<?, ?, ?> simmonsSource, Tap<?, ?, ?> normalizedSink) {
        Pipe numPipe = new Each("split", Fields.ALL, new CustomSplitFunction<>(new Fields("group", "node_type", "node_number", "node_name")), Fields.RESULTS);
        ExpressionFilter nodeNameFilter = new ExpressionFilter("node_name.toLowerCase().contains(\"null\")", String.class);
        numPipe = new Each(numPipe, new Fields("node_name"), nodeNameFilter);
        numPipe = new Unique(numPipe, new Fields("node_type", "node_number", "node_name"));
        numPipe = new GroupBy(numPipe, new Fields("group"));
        numPipe = new Every(numPipe, new Fields("group"), new EnumeratorBuffer(new Fields("id")), Fields.SWAP);
        return FlowDef.flowDef()//
                .addSource(numPipe, simmonsSource) //
                .addTail(numPipe)//
                .addSink(numPipe, normalizedSink);
    }
}
