package document.examples;

import document.datatypes.DocumentCollection;
import document.operatorsimpl.r2r.RelationToRelationDocumentSelection;
import document.operatorsimpl.r2s.RelationToStreamDocument;
import document.stream.DocumentStream;
import document.stream.DocumentStreamGenerator;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.MBHoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.state.MapMultiBufferState;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;

import java.util.ArrayList;
import java.util.List;

public class document_example {
    public static void main(String[] args) throws InterruptedException {

        DocumentStreamGenerator generator = new DocumentStreamGenerator();

        DataStream<String> inputStream_1 = generator.getStream("http://test/stream1");
        // define output stream
        DataStream<String> outStream = new DocumentStream<>("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Tick tick = Tick.TIME_DRIVEN;
        Time instance = new TimeImpl(0);
        DocumentCollection emptyContent = new DocumentCollection();

        AccumulatorContentFactory<String, String, DocumentCollection> accumulatorContentFactory = new AccumulatorContentFactory<>(
                t -> t,
                (t) -> {
                    DocumentCollection dc = new DocumentCollection();
                    dc.addElement(t);
                    return dc;
                },
                (d1, d2) -> d1.append(d2),
                emptyContent
        );


        ContinuousProgram<String, String, DocumentCollection, String> cp = new ContinuousProgramImpl<>();

        StreamToRelationOperator<String, DocumentCollection> s2rOp_1 =
                new MBHoppingWindowOpImpl<>(
                        tick,
                        instance,
                        "w1",
                        new MapMultiBufferState<>(accumulatorContentFactory),
                        report,
                        1000,
                        1000);


        RelationToRelationOperator<DocumentCollection> r2rOp = new RelationToRelationDocumentSelection(List.of("w1"), "partial_1");
        RelationToStreamOperator<DocumentCollection, String> r2sOp = new RelationToStreamDocument();

        Task<String, String, DocumentCollection, String> task = new TaskImpl<>("1");
        task = task.addS2ROperator(s2rOp_1, inputStream_1)
                .addR2ROperator(r2rOp)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSDefault<>())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        task.initialize();

        List<DataStream<String>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStream_1);


        List<DataStream<String>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        cp.buildTask(task, inputStreams, outputStreams);

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();
    }

}
