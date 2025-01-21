package nexmark.queries.tablesaw;

import nexmark.content.LogicalSlidingContentFactory;
import nexmark.customdatatypes.tablesaw.TimestampedElement;
import nexmark.operators.r2r.tablesaw.R2Rq8;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.EvictOnReportWindow;
import nexmark.operators.s2r.LogicalSlidingWindow;
import nexmark.report.Never;
import nexmark.report.Periodic;
import nexmark.stream.StreamGenerator;
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
import nexmark.utils.MyTask;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query8 implements Query {

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

        StreamGenerator generator = new StreamGenerator();

        DataStream<TimestampedElement<Table>> auction = generator.getStream("Auction");
        DataStream<TimestampedElement<Table>> bid = generator.getStream("Bid");
        DataStream<TimestampedElement<Table>> person = generator.getStream("Person");

        // define output stream
        DataStream<Row> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(10));

        Report neverReport = new ReportImpl();
        neverReport.add( new Never());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create("empty");

        AccumulatorContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> contentFactory = new AccumulatorContentFactory<>(
                (t->t),
                (t->t.getElement().copy()),
                ((t1, t2)->t1.isEmpty()?t2:t1.append(t2)),
                emptyContent);

        LogicalSlidingContentFactory<Table, Table> slidingContentFactory = new LogicalSlidingContentFactory<>(
                emptyContent,
                2000,
                t->t.getElement().copy(),
                (t1, t2)->t1.isEmpty()?t2:t1.append(t2)

        );

        ContinuousProgram<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> auctionWindow =
                new EvictOnReportWindow<>(
                        instance,
                        "auctionWindow",
                        contentFactory,
                        report);

        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> personWindow =
                new LogicalSlidingWindow<>(
                        instance,
                        "personWindow",
                        slidingContentFactory,
                        neverReport,
                        2000);

        RelationToRelationOperator<Table> r2r = new R2Rq8(List.of("personWindow", "auctionWindow"), "res");

        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> task = new MyTask<>("1");
        task = task.addS2ROperator(auctionWindow, auction)
                .addS2ROperator(personWindow, person)
                .addR2ROperator(r2r)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        task.initialize();

        List<DataStream<TimestampedElement<Table>>> inputStreams = new ArrayList<>();
        inputStreams.add(bid);
        inputStreams.add(auction);
        inputStreams.add(person);


        List<DataStream<Row>> outputStreams = new ArrayList<>();
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
