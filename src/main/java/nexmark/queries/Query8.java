package nexmark.queries;

import custom.customoperators.CustomTumblingWindow;
import nexmark.content.LogicalSlidingContentFactory;
import nexmark.customdatatypes.TimestampedElement;
import nexmark.operators.r2r.R2Rq8;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.LogicalSlidingWindow;
import nexmark.report.Periodic;
import nexmark.stream.StreamGenerator;
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
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query8 {

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

    public static void main(String[] args) throws InterruptedException {

        StreamGenerator generator = new StreamGenerator();

        DataStream<TimestampedElement<Table>> auction = generator.getStream("Auction");
        DataStream<TimestampedElement<Table>> bid = generator.getStream("Bid");
        DataStream<TimestampedElement<Table>> person = generator.getStream("Person");

        // define output stream
        DataStream<Row> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create("empty");

        AccumulatorContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> contentFactory = new AccumulatorContentFactory<>(
                (t->t),
                (t->t.getElement().copy()),
                ((t1, t2)->t1.isEmpty()?t2:t1.append(t2)),
                emptyContent);

        ContinuousProgram<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> auctionWindow =
                new CustomTumblingWindow<>(
                        instance,
                        "auctionWindow",
                        contentFactory,
                        report,
                        2000);

        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> personWindow =
                new CustomTumblingWindow<>(
                        instance,
                        "personWindow",
                        contentFactory,
                        report,
                        2000);

        RelationToRelationOperator<Table> r2r = new R2Rq8(List.of("personWindow", "auctionWindow"), "res");

        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> task = new TaskImpl<>("1");
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

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();

    }

}
