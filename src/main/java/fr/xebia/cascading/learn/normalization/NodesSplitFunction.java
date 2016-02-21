package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import static fr.xebia.cascading.learn.normalization.ColumnsNames.*;

public class NodesSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
    private static final long serialVersionUID = 1L;
    private static final int SPLIT_CONSTANT = 1;

    public NodesSplitFunction(Fields wordField) {
        super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
                        FunctionCall<Context> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple section = createTuple(arguments, SECTION_NUMBER, SECTION_NAME);
        Tuple subSection = createTuple(arguments, SUB_SECTION_NUMBER, SUB_SECTION_NAME);
        Tuple question = createTuple(arguments, QUESTION_NUMBER, QUESTION);
        Tuple option = createTuple(arguments, OPTION_NUMBER, OPTION_NAME);
        Tuple answer = createTuple(arguments, null, ANSWER);

        functionCall.getOutputCollector().add(section);
        functionCall.getOutputCollector().add(subSection);
        functionCall.getOutputCollector().add(question);
        functionCall.getOutputCollector().add(option);
        functionCall.getOutputCollector().add(answer);
    }

    private Tuple createTuple(TupleEntry tupleEntry, String nodeNumber, String nodeName) {
        Tuple tuple = new Tuple();
        tuple.add(SPLIT_CONSTANT);
        tuple.add(nodeName);
        tuple.add(nodeNumber == null ? "NULL" : getFieldValue(tupleEntry, nodeNumber));
        tuple.add(getFieldValue(tupleEntry, nodeName));
        return tuple;
    }

    private String getFieldValue(TupleEntry tupleEntry, String fieldName) {
        return tupleEntry.selectTuple(new Fields(fieldName)).getString(0).trim();
    }
}
