package nexmark.queries.custom;

import nexmark.content.LogicalSlidingContentFactory;
import nexmark.content.custom.FastAccumulatorFactory;
import nexmark.content.custom.FastLogicalSlidingFactory;
import nexmark.content.custom.Q8SlidingFactory;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.tablesaw.TimestampedElement;
import nexmark.operators.r2r.custom.R2Rq8;
import nexmark.operators.r2s.R2SCustom;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.EvictOnReportWindow;
import nexmark.operators.s2r.LogicalSlidingWindow;
import nexmark.report.Never;
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

public class Query8Custom implements Query {

       /*
        This query finds people who put something up for
        sale within twelve hours of registering to use the auc-
        tion service. This query could be used to track new
        users for user followup or to make sure the new users
        are “behaving”. This query uses a sliding window join
        over a logical or time-based window.
        SELECT person.id, person.name
        FROM person [RANGE 12 HOURS PRECEDING],
        open auction [RANGE 12 HOURS PRECEDING]
        WHERE person.id = open auction.sellerId;
       */

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
        report.add(new Periodic(10));

        Report neverReport = new ReportImpl();
        neverReport.add( new Never());

        Time instance = new TimeImpl(0);

        FastAccumulatorFactory contentFactory = new FastAccumulatorFactory();

        Q8SlidingFactory slidingContentFactory = new Q8SlidingFactory(
                2000
        );

        ContinuousProgram<Entity, Entity, List<Entity>, Entity> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<Entity, Entity, List<Entity>> auctionWindow =
                new EvictOnReportWindow<>(
                        instance,
                        "auctionWindow",
                        contentFactory,
                        report);

        StreamToRelationOperator<Entity, Entity, List<Entity>> personWindow =
                new LogicalSlidingWindow<>(
                        instance,
                        "personWindow",
                        slidingContentFactory,
                        neverReport,
                        200);

        RelationToRelationOperator<List<Entity>> r2r = new R2Rq8(List.of("personWindow", "auctionWindow"), "res");

        RelationToStreamOperator<List<Entity>, Entity> r2sOp = new R2SCustom();

        Task<Entity, Entity, List<Entity>, Entity> task = new MyTask<>("1");
        task = task.addS2ROperator(auctionWindow, auction)
                .addS2ROperator(personWindow, person)
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
