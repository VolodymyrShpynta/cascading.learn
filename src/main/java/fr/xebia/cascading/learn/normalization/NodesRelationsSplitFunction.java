package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Optional;

import static fr.xebia.cascading.learn.normalization.ColumnsNames.PUNCH_CODE;

public class NodesRelationsSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
    private static final long serialVersionUID = 1L;

    public NodesRelationsSplitFunction(Fields wordField) {
        super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
                        FunctionCall<Context> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        NodeDefinition node = NodeDefinition.SECTION;
        Optional<NodeDefinition> child = getChild(arguments, node);
        while (child.isPresent()) {
            functionCall.getOutputCollector().add(
                    createTuple(arguments, node, child.get())
            );
            node = child.get();
            child = getChild(arguments, node);
        }
    }

    private Tuple createTuple(TupleEntry tupleEntry, NodeDefinition parent, NodeDefinition child) {
        Tuple tuple = new Tuple();
        tuple.add(parent.getName());
        tuple.add(getFieldValue(tupleEntry, parent.getNumber()));
        tuple.add(getFieldValue(tupleEntry, parent.getName()));
        tuple.add(child.getName());
        tuple.add(NodeDefinition.ANSWER.equals(child) ? "NULL" : getFieldValue(tupleEntry, child.getNumber()));
        tuple.add(getFieldValue(tupleEntry, child.getName()));
        tuple.add(NodeDefinition.ANSWER.equals(child) ? getFieldValue(tupleEntry, PUNCH_CODE) : "NULL");
        return tuple;
    }

    private String getFieldValue(TupleEntry tupleEntry, String fieldName) {
        return tupleEntry.selectTuple(new Fields(fieldName)).getString(0).trim();
    }

    private Optional<NodeDefinition> getChild(TupleEntry tupleEntry, NodeDefinition node) {
        Optional<NodeDefinition> childNode = NodeDefinition.getNext(node);
        while (childNode.isPresent()) {
            final String childFieldValue = getFieldValue(tupleEntry, childNode.get().getName());
            if (!childFieldValue.isEmpty() && !childFieldValue.toLowerCase().contains("[null]")) {
                return childNode;
            }
            childNode = NodeDefinition.getNext(childNode.get());
        }
        return Optional.empty();
    }
}