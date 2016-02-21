package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Arrays;
import java.util.Optional;

import static fr.xebia.cascading.learn.normalization.ColumnsNames.*;

public class NodesRelationsSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
    private static final long serialVersionUID = 1L;

    public NodesRelationsSplitFunction(Fields wordField) {
        super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
                        FunctionCall<Context> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        NodeDefinition parent = NodeDefinition.SECTION;
        Optional<NodeDefinition> child = getChild(arguments, parent);
        while (child.isPresent()) {
            functionCall.getOutputCollector().add(
                    createTuple(arguments, parent, child.get())
            );
            parent = child.get();
            child = getChild(arguments, parent);
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
        int nodeIndex = Arrays.asList(NodeDefinition.values()).indexOf(node);
        for (int i = nodeIndex + 1; i < NodeDefinition.values().length; i++) {
            NodeDefinition childNode = NodeDefinition.values()[i];
            final String fieldValue = getFieldValue(tupleEntry, childNode.getName());
            if (!fieldValue.isEmpty() && !fieldValue.toLowerCase().contains("[null]")) {
                return Optional.of(childNode);
            }
        }
        return Optional.empty();
    }
}