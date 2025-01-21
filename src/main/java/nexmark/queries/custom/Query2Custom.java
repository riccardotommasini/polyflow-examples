package nexmark.queries.custom;

import nexmark.content.custom.FastAccumulatorFactory;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.tablesaw.TimestampedElement;
import nexmark.operators.r2r.custom.R2Rq2;
import nexmark.operators.r2s.R2SCustom;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.EvictOnReportWindow;
import nexmark.report.Periodic;
import nexmark.stream.StreamGenerator;
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
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

/*
    Query 2 selects all bids on a set of five items and
    tests the stream system’s selection operation.

    SELECT itemid, price
    FROM bid
    WHERE itemid = 1007 OR
    itemid = 1020 OR
    itemid = 2001 OR
    itemid = 2019 OR
    itemid = 1087;
 */
public class Query2Custom implements Query {
    public double throughput;
    public double totalTime;
    public double timeSpentParsing;
    public void execute(){

        StreamGeneratorCustom generator = new StreamGeneratorCustom();

        DataStream<Entity> auction = generator.getStream("Auction");
        DataStream<Entity> bid = generator.getStream("Bid");
        DataStream<Entity> person = generator.getStream("Person");

        // define output stream
        DataStream<Entity> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(1)); //TODO: review output strategy

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        //The sliding factor should be the same as the window size
        FastAccumulatorFactory contentFactory = new FastAccumulatorFactory();

        ContinuousProgram<Entity, Entity, List<Entity>, Entity> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<Entity, Entity, List<Entity>> bidWindow =
                new EvictOnReportWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report);


        RelationToRelationOperator<List<Entity>> r2r = new R2Rq2(List.of("bidWindow"), "res");

        RelationToStreamOperator<List<Entity>, Entity> r2sOp = new R2SCustom();

        Task<Entity, Entity, List<Entity>, Entity> task = new MyTask<>("1");
        task = task
                .addS2ROperator(bidWindow, bid)
                .addR2ROperator(r2r)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSDefault<>())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        task.initialize();

        List<DataStream<Entity>> inputStreams = new ArrayList<>();
        inputStreams.add(bid);
        inputStreams.add(auction);
        inputStreams.add(person);


        List<DataStream<Entity>> outputStreams = new ArrayList<>();
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


