package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Optional;

public class NodesSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
    private static final long serialVersionUID = 1L;
    private static final int FAKE_FIELD_USED_FOR_GROUPING = 1;

    public NodesSplitFunction(Fields wordField) {
        super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
                        FunctionCall<Context> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Optional<NodeDefinition> node = Optional.of(NodeDefinition.SECTION);
        do {
            functionCall.getOutputCollector().add(
                    createTuple(arguments, node.get()));
            node = NodeDefinition.getNext(node.get());
        } while (node.isPresent());
    }

    private Tuple createTuple(TupleEntry tupleEntry, NodeDefinition node) {
        Tuple tuple = new Tuple();
        tuple.add(FAKE_FIELD_USED_FOR_GROUPING);
        tuple.add(node.getName());
        tuple.add(NodeDefinition.ANSWER.equals(node) ? "NULL" : getFieldValue(tupleEntry, node.getNumber()));
        tuple.add(getFieldValue(tupleEntry, node.getName()));
        return tuple;
    }

    private String getFieldValue(TupleEntry tupleEntry, String fieldName) {
        return tupleEntry.selectTuple(new Fields(fieldName)).getString(0).trim();
    }
}
