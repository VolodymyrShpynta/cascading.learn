package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.tuple.Fields;


public class NormalizationDataflow {

    public static FlowDef normalize(Tap<?, ?, ?> simmonsSource, Tap<?, ?, ?> normalizedSink) {
        Pipe pipe = new Each("split", Fields.ALL, new CustomSplitFunction<>(new Fields("group", "node_type", "node_number", "node_name")), Fields.RESULTS);
        ExpressionFilter nodeNameFilter = new ExpressionFilter("node_name.toLowerCase().contains(\"null\")", String.class);
        pipe = new Each(pipe, new Fields("node_name"), nodeNameFilter);
        pipe = new GroupBy(pipe, new Fields("group"));
        pipe = new Every(pipe, new Fields("group"), new EnumeratorBuffer(new Fields("id")), Fields.SWAP);
        return FlowDef.flowDef()//
                .addSource(pipe, simmonsSource) //
                .addTail(pipe)//
                .addSink(pipe, normalizedSink);
    }
}
