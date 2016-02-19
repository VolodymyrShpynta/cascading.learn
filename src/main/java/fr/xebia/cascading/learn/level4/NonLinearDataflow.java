package fr.xebia.cascading.learn.level4;

import cascading.flow.FlowDef;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.InnerJoin;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * Up to now, operations were stacked one after the other. But the dataflow can
 * be non linear, with multiples sources, multiples sinks, forks and merges.
 */
public class NonLinearDataflow {

    /**
     * Use {@link CoGroup} in order to know the party of each presidents.
     * You will need to create (and bind) one Pipe per source.
     * You might need to correct the schema in order to match the expected results.
     * <p/>
     * presidentsSource field(s) : "year","president"
     * partiesSource field(s) : "year","party"
     * sink field(s) : "president","party"
     *
     * @see http://docs.cascading.org/cascading/3.0/userguide/ch05-pipe-assemblies.html#_cogroup
     */
    public static FlowDef cogroup(Tap<?, ?, ?> presidentsSource, Tap<?, ?, ?> partiesSource,
                                  Tap<?, ?, ?> sink) {
        Pipe presidentsPipe = new Pipe("presidentsPipe");
        Pipe partiesPipe = new Pipe("partiesPipe");
        Fields commonFields = new Fields("year");
        Fields newFields = new Fields("year1", "president", "year2", "party");
        Pipe joinedPipe = new CoGroup(presidentsPipe, commonFields, partiesPipe, commonFields, newFields, new InnerJoin());
        return FlowDef.flowDef()
                .addSource(presidentsPipe, presidentsSource)
                .addSource(partiesPipe, partiesSource)
                .addTail(joinedPipe)
                .addSink(joinedPipe, sink);
    }

    /**
     * Split the input in order use a different sink for each party. There is no
     * specific operator for that, use the same Pipe instance as the parent.
     * You will need to create (and bind) one named Pipe per sink.
     * <p/>
     * source field(s) : "president","party"
     * gaullistSink field(s) : "president","party"
     * republicanSink field(s) : "president","party"
     * socialistSink field(s) : "president","party"
     * <p/>
     * In a different context, one could use {@link PartitionTap} in order to arrive to a similar results.
     *
     * @see http://docs.cascading.org/cascading/3.0/userguide/ch15-advanced.html#partition-tap
     */
    public static FlowDef split(Tap<?, ?, ?> source,
                                Tap<?, ?, ?> gaullistSink, Tap<?, ?, ?> republicanSink, Tap<?, ?, ?> socialistSink) {
        ExpressionFilter gaullistFilter = new ExpressionFilter("!party.contains(\"Gaullist\")", String.class);
        Pipe gaullistPipe = new Each("gaullistPipe", new Fields("president", "party"), gaullistFilter);

        ExpressionFilter republicanFilter = new ExpressionFilter("!party.contains(\"Republican\")", String.class);
        Pipe republicanPipe = new Each("republicanPipe", new Fields("president", "party"), republicanFilter);

        ExpressionFilter socialistFilter = new ExpressionFilter("!party.contains(\"Socialist\")", String.class);
        Pipe socialistPipe = new Each("socialistPipe", new Fields("president", "party"), socialistFilter);

        return FlowDef.flowDef()//
                .addSource(gaullistPipe, source) //
                .addSource(republicanPipe, source)
                .addSource(socialistPipe, source)
                .addTail(gaullistPipe)//
                .addTail(republicanPipe)
                .addTail(socialistPipe)
                .addSink(gaullistPipe, gaullistSink)
                .addSink(republicanPipe, republicanSink)
                .addSink(socialistPipe, socialistSink);
    }
}
