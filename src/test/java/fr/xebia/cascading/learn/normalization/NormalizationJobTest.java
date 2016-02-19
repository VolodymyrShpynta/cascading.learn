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
        Scheme simmonsSchemeDefinition = new TextDelimited(new Fields("section_number", "section_name", "sub_section_number", "sub_section_name", "question_number", "question", "option_number", "option_name", "punch_code", "answer"), true, ",");
        Tap<?, ?, ?> simmonsSource = new FileTap(simmonsSchemeDefinition, presidentsPath);

        // actual output of the job
        String sinkPath = "target/normalization/normalized.csv";
        Scheme sinkScheme = new TextDelimited(true, ",");
        Tap<?, ?, ?> normalizedSink = new FileTap(sinkScheme, sinkPath, SinkMode.REPLACE);

        // create the job definition, and run it
        FlowDef flowDef = NormalizationDataflow.normalize(simmonsSource, normalizedSink);
        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();

        Assert.sameContent(sinkPath, "src/test/resources/normalization/expectation.csv");
    }
}
