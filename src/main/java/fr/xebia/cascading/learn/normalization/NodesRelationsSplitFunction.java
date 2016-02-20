package fr.xebia.cascading.learn.normalization;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class NodesRelationsSplitFunction<Context> extends BaseOperation<Context> implements Function<Context> {
    private static final long serialVersionUID = 1L;

    public NodesRelationsSplitFunction(Fields wordField) {
        super(1, wordField);
    }

    @Override
    public void operate(@SuppressWarnings("rawtypes") FlowProcess flowProcess,
                        FunctionCall<Context> functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        Tuple section_subSection = createTuple(arguments,
                "section_number", "section_name",
                "sub_section_number", "sub_section_name",
                null);

        Tuple subSection_question = createTuple(arguments,
                "sub_section_number", "sub_section_name",
                "question_number", "question",
                null);

        Tuple question_option = createTuple(arguments,
                "question_number", "question",
                "option_number", "option_name",
                null);
        Tuple option_answer = createTuple(arguments,
                "option_number", "option_name",
                null, "answer",
                "punch_code");

        functionCall.getOutputCollector().add(section_subSection);
        functionCall.getOutputCollector().add(subSection_question);
        functionCall.getOutputCollector().add(question_option);
        functionCall.getOutputCollector().add(option_answer);
    }

    private Tuple createTuple(TupleEntry tupleEntry,
                              String nodeNumber1, String nodeName1,
                              String nodeNumber2, String nodeName2,
                              String panchCode) {
        Tuple tuple = new Tuple();
        tuple.add(nodeName1);
        tuple.add(nodeNumber1 == null ? "NULL" : getFieldValue(tupleEntry, nodeNumber1));
        tuple.add(getFieldValue(tupleEntry, nodeName1));
        tuple.add(nodeName2);
        tuple.add(nodeNumber2 == null ? "NULL" : getFieldValue(tupleEntry, nodeNumber2));
        tuple.add(getFieldValue(tupleEntry, nodeName2));
        tuple.add(panchCode == null ? "NULL" : getFieldValue(tupleEntry, panchCode));
        return tuple;
    }

    private String getFieldValue(TupleEntry tupleEntry, String fieldName) {
        return tupleEntry.selectTuple(new Fields(fieldName)).getString(0).trim();
    }
}
