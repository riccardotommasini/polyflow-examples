package nexmark.queries;

import nexmark.content.custom.FastAccumulatorFactory;
import nexmark.operators.r2r.custom.R2Rq1;
import nexmark.operators.r2s.R2SCustom;
import nexmark.operators.s2r.EvictOnReportWindow;
import nexmark.report.Periodic;
import nexmark.stream.StreamGeneratorCustom;
import nexmark.utils.MyTask;
import nexmark.utils.Query;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;
import relational.stream.RowStream;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Query1Custom implements Query {

    public double throughput;
    public double totalTime;
    public double timeSpentParsing;
    public void execute(){

        StreamGeneratorCustom generator = new StreamGeneratorCustom();

        DataStream<Serializable> auction = generator.getStream("Auction");
        DataStream<Serializable> bid = generator.getStream("Bid");
        DataStream<Serializable> person = generator.getStream("Person");

        // define output stream
        DataStream<Serializable> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(1)); //TODO: review output strategy

        Time instance = new TimeImpl(0);
        List<Serializable> emptyContent = new ArrayList<>();

        //The sliding factor should be the same as the window size
        FastAccumulatorFactory contentFactory = new FastAccumulatorFactory();

        ContinuousProgram<Serializable, Serializable, List<Serializable>, Serializable> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<Serializable, Serializable, List<Serializable>> bidWindow =
                new EvictOnReportWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report);


        RelationToRelationOperator<List<Serializable>> r2r = new R2Rq1(List.of("bidWindow"), "res");

        RelationToStreamOperator<List<Serializable>, Serializable> r2sOp = new R2SCustom();

        Task<Serializable, Serializable, List<Serializable>, Serializable> task = new MyTask<>("1");
        task = task
                .addS2ROperator(bidWindow, bid)
                .addR2ROperator(r2r)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSDefault<>())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        task.initialize();

        List<DataStream<Serializable>> inputStreams = new ArrayList<>();
        inputStreams.add(bid);
        inputStreams.add(auction);
        inputStreams.add(person);


        List<DataStream<Serializable>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        cp.buildTask(task, inputStreams, outputStreams);

        outStream.addConsumer((out, el, ts) -> {});

        generator.startStreaming();

        this.totalTime = generator.totalTime;
        this.throughput = generator.throughput;
        this.timeSpentParsing = generator.timeSpentParsing;

    }

    @Override
    public double getTotalTime() {
        return totalTime;
    }

    @Override
    public double getThroughput() {
        return throughput;
    }

    @Override
    public double getTimeSpentParsing() {
        return timeSpentParsing;
    }
}
