package fr.xebia.cascading.learn.level4;

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

public class NonLinearDataflowTest {

    @Before
    public void doNotCareAboutOsStuff() {
        System.setProperty("line.separator", "\n");
    }

    @Test
    public void learnToCoGroup() throws Exception {
        // inputs of the job
        String presidentsPath = "src/test/resources/fifth-france-presidents.txt";
        Tap<?, ?, ?> presidentsSource = new FileTap(new TextDelimited(true, "\t"), presidentsPath);

        String partiesPath = "src/test/resources/fifth-france-parties.txt";
        Tap<?, ?, ?> partiesSource = new FileTap(new TextDelimited(true, "\t"), partiesPath);

        // actual output of the job
        String sinkPath = "target/level4/cogroup.txt";
        Scheme sinkScheme = new TextDelimited(new Fields("president", "party"), true, "\t");
        Tap<?, ?, ?> sink = new FileTap(sinkScheme, sinkPath, SinkMode.REPLACE);

        // create the job definition, and run it
        FlowDef flowDef = NonLinearDataflow.cogroup(presidentsSource, partiesSource, sink);
        new LocalFlowConnector().connect(flowDef).complete();

        // check that actual and expect outputs are the same
        Assert.sameContent(sinkPath, "src/test/resources/level4/cogroup/expectation.txt");
    }

    @Test
    public void myLearnToCoGroup() throws Exception {
        // inputs of the job
        String presidentsPath = "src/test/resources/fifth-france-presidents.txt";
        Scheme presidentsSchemeDefinition = new TextDelimited(new Fields("year", "president"), true, "\t");
        Tap<?, ?, ?> presidentsSource = new FileTap(presidentsSchemeDefinition, presidentsPath);

        String partiesPath = "src/test/resources/fifth-france-parties.txt";
        Scheme partiesSchemeDefinition = new TextDelimited(new Fields("year", "party"), true, "\t");
        Tap<?, ?, ?> partiesSource = new FileTap(partiesSchemeDefinition, partiesPath);

        // actual output of the job
        String sinkPath = "target/level4/cogroup.txt";
        Scheme sinkScheme = new TextDelimited(new Fields("president", "party"), true, "\t");
        Tap<?, ?, ?> sink = new FileTap(sinkScheme, sinkPath, SinkMode.REPLACE);

        // create the job definition, and run it
        FlowDef flowDef = NonLinearDataflow.cogroup(presidentsSource, partiesSource, sink);
        FlowConnector flowConnector = new LocalFlowConnector();
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();
        //OPTIONAL: Flow diagram
        flow.writeDOT(sinkPath + "target/level4/flowdiagram3.dot");

        Assert.sameContent(sinkPath, "src/test/resources/level4/cogroup/expectation.txt");
    }

    @Test
    public void learnToSplit() throws Exception {
        // input of the job
        String sourcePath = "src/test/resources/level4/cogroup/expectation.txt";
        Scheme sourceScheme = new TextDelimited(new Fields("president", "party"), true, "\t");
        Tap<?, ?, ?> source = new FileTap(sourceScheme, sourcePath);

        // actual outputs of the job
        String gaullistPath = "target/level4/split-gaullist.txt";
        Tap<?, ?, ?> gaullistSink = new FileTap(new TextDelimited(true, "\t"), gaullistPath, SinkMode.REPLACE);

        String republicanPath = "target/level4/split-republican.txt";
        Tap<?, ?, ?> republicanSink = new FileTap(new TextDelimited(true, "\t"), republicanPath, SinkMode.REPLACE);

        String socialistPath = "target/level4/split-socialist.txt";
        Tap<?, ?, ?> socialistSink = new FileTap(new TextDelimited(true, "\t"), socialistPath, SinkMode.REPLACE);

        // create the job definition, and run it
        FlowDef flowDef = NonLinearDataflow.split(source, gaullistSink, republicanSink, socialistSink);
        new LocalFlowConnector().connect(flowDef).complete();

        // check that actual and expect outputs are the same
        Assert.sameContent(gaullistPath, "src/test/resources/level4/split/expectation-gaullist.txt");
        Assert.sameContent(republicanPath, "src/test/resources/level4/split/expectation-republican.txt");
        Assert.sameContent(socialistPath, "src/test/resources/level4/split/expectation-socialist.txt");
    }
}
