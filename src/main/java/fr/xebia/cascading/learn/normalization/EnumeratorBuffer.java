package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * Created by Volodymyr Shpynta on 20.02.2016.
 */
public class EnumeratorBuffer extends BaseOperation implements Buffer {

    public EnumeratorBuffer() {
        super(1, new Fields("id"));
    }

    public EnumeratorBuffer(Fields fieldDeclaration) {
        super(1, fieldDeclaration);
    }

    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        // init the count
        long count = 1;
        // get all the current argument values for this grouping
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        while (arguments.hasNext()) {
            final TupleEntry next = arguments.next();
            bufferCall.getOutputCollector().add(new Tuple(count));
            count++;
        }
    }
}
