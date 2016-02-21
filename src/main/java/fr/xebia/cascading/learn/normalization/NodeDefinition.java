package fr.xebia.cascading.learn.normalization;

import java.util.Arrays;
import java.util.Optional;

/**
 * Created by Volodymyr Shpynta on 21.02.2016.
 */
public enum NodeDefinition {
    SECTION(ColumnsNames.SECTION_NUMBER, ColumnsNames.SECTION_NAME),
    SUB_SECTION(ColumnsNames.SUB_SECTION_NUMBER, ColumnsNames.SUB_SECTION_NAME),
    QUESTION(ColumnsNames.QUESTION_NUMBER, ColumnsNames.QUESTION),
    OPTION(ColumnsNames.OPTION_NUMBER, ColumnsNames.OPTION_NAME),
    ANSWER(null, ColumnsNames.ANSWER);

    NodeDefinition(String number, String name) {
        this.number = number;
        this.name = name;
    }

    private String number;
    private String name;

    private static NodeDefinition[] enumValues = values();

    public static Optional<NodeDefinition> getNext(NodeDefinition nodeDefinition) {
        int nodeIndex = Arrays.asList(enumValues).indexOf(nodeDefinition);
        if (nodeIndex + 1 < enumValues.length) {
            return Optional.of(NodeDefinition.values()[nodeIndex + 1]);
        }
        return Optional.empty();
    }

    public String getNumber() {
        return number;
    }

    public String getName() {
        return name;
    }
}
