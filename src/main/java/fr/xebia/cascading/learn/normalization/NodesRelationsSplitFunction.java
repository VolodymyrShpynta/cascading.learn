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

public class NodesRelationsSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
    private static final long serialVersionUID = 1L;

    public NodesRelationsSplitFunction(Fields wordField) {
        super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
                        FunctionCall<Context> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Node parent = Node.SECTION;
        Optional<Node> child = getChild(arguments, parent);
        while (child.isPresent()) {
            functionCall.getOutputCollector().add(
                    createTuple(arguments, parent, child.get())
            );
            parent = child.get();
            child = getChild(arguments, parent);
        }
    }

    private Tuple createTuple(TupleEntry tupleEntry, Node parent, Node child) {
        Tuple tuple = new Tuple();
        tuple.add(parent.name);
        tuple.add(getFieldValue(tupleEntry, parent.number));
        tuple.add(getFieldValue(tupleEntry, parent.name));
        tuple.add(child.name);
        tuple.add(Node.ANSWER.equals(child) ? "NULL" : getFieldValue(tupleEntry, child.number));
        tuple.add(getFieldValue(tupleEntry, child.name));
        tuple.add(Node.ANSWER.equals(child) ? getFieldValue(tupleEntry, "punch_code") : "NULL");
        return tuple;
    }

    private String getFieldValue(TupleEntry tupleEntry, String fieldName) {
        return tupleEntry.selectTuple(new Fields(fieldName)).getString(0).trim();
    }

    private Optional<Node> getChild(TupleEntry tupleEntry, Node node) {
        int nodeIndex = Arrays.asList(Node.values()).indexOf(node);
        for (int i = nodeIndex + 1; i < Node.values().length; i++) {
            Node childNode = Node.values()[i];
            final String fieldValue = getFieldValue(tupleEntry, childNode.name);
            if (!fieldValue.isEmpty() && !fieldValue.toLowerCase().contains("[null]")) {
                return Optional.of(childNode);
            }
        }
        return Optional.empty();
    }

    private enum Node {
        SECTION("section_number", "section_name"),
        SUB_SECTION("sub_section_number", "sub_section_name"),
        QUESTION("question_number", "question"),
        OPTION("option_number", "option_name"),
        ANSWER(null, "answer");

        Node(String number, String name) {
            this.number = number;
            this.name = name;
        }

        private String number;
        private String name;
    }
}
