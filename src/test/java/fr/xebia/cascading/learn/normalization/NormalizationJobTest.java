package fr.xebia.cascading.learn.normalization;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import fr.xebia.cascading.learn.Assert;
import org.junit.Before;
import org.junit.Test;

import static fr.xebia.cascading.learn.normalization.ColumnsNames.*;

/**
 * Created by Volodymyr Shpynta on 19.02.2016.
 */
public class NormalizationJobTest {
    @Before
    public void doNotCareAboutOsStuff() {
        System.setProperty("line.separator", "\n");
    }

    @Test
    public void normalizeSimmonsData() throws Exception {
        // inputs of the job
        String presidentsPath = "src/test/resources/normalization/simmons-export.csv";
        Scheme simmonsSchemeDefinition = new TextDelimited(new Fields(SECTION_NUMBER, SECTION_NAME, SUB_SECTION_NUMBER, SUB_SECTION_NAME, QUESTION_NUMBER, QUESTION, OPTION_NUMBER, OPTION_NAME, PUNCH_CODE, ANSWER), true, ",");
        Tap<?, ?, ?> simmonsSource = new FileTap(simmonsSchemeDefinition, presidentsPath);

        // actual output of the job
        String nodesRelationsPath = "target/normalization/nodes-relations.csv";
        Scheme nodesRelationsScheme = new TextDelimited(true, ",");
        Tap<?, ?, ?> nodesRelationsSink = new FileTap(nodesRelationsScheme, nodesRelationsPath, SinkMode.REPLACE);

        String nodesPath = "target/normalization/nodes.csv";
        Scheme nodesScheme = new TextDelimited(true, ",");
        Tap<?, ?, ?> nodesSink = new FileTap(nodesScheme, nodesPath, SinkMode.REPLACE);

        // create the job definition, and run it
        FlowDef flowDef = NormalizationDataflow.normalize(simmonsSource, nodesSink, nodesRelationsSink);
        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();

        Assert.sameContent(nodesPath, "src/test/resources/normalization/nodes-expectation.csv");
        Assert.sameContent(nodesRelationsPath, "src/test/resources/normalization/nodes-relations-expectation.csv");
    }
}
