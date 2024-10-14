package nexmark.queries;

import nexmark.content.LogicalSlidingContentFactory;
import nexmark.content.PhysicalSlidingContentFactory;
import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.LogicalSlidingWindow;
import nexmark.operators.s2r.PhysicalSlidingWindow;
import nexmark.report.Periodic;
import nexmark.stream.StreamGenerator;
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
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query6 {

        /*
        Query 6 calculates, for each seller, the average sell-
        ing price of items sold by that seller. For example, auc-
        tion site administrators may be interested in knowing
        which users sell the highest price items. This query
        uses an event-based, sliding window group by.
        SELECT AVG(CA.price), CA.sellerId
        FROM closed auction CA
        [PARTITION BY CA.sellerId
        ROWS 10 PRECEDING]
        */
    public static void main(String[] args) throws InterruptedException {

        StreamGenerator generator = new StreamGenerator();

        DataStream<TestTimestampedRow> auction = generator.getStream("Auction");
        DataStream<TestTimestampedRow> bid = generator.getStream("Bid");
        DataStream<TestTimestampedRow> person = generator.getStream("Person");

        // define output stream
        DataStream<Row> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(200));

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        LogicalSlidingContentFactory contentFactory_person = new LogicalSlidingContentFactory(emptyContent, 500);

        LogicalSlidingContentFactory contentFactory_bid = new LogicalSlidingContentFactory(emptyContent, 500);

        //auctions expire in around 30 seconds, we keep a sliding window over them with that size
        PhysicalSlidingContentFactory contentFactory_auction = new PhysicalSlidingContentFactory(emptyContent, 10);

        ContinuousProgram<TestTimestampedRow, TestTimestampedRow, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> auctionWindow =
                new PhysicalSlidingWindow<>(
                        instance,
                        "auctionWindow",
                        contentFactory_auction,
                        report);

        //Auctions last on average 30 seconds, so we keep bids in a sliding window of that size
        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> bidWindow =
                new LogicalSlidingWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory_bid,
                        report,
                        30000);
        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> personWindow =
                new LogicalSlidingWindow<>(
                        instance,
                        "personWindow",
                        contentFactory_person,
                        report,
                        500);

        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TestTimestampedRow, TestTimestampedRow, Table, Row> task = new TaskImpl<>();
        task = task.addS2ROperator(auctionWindow, auction)
                .addS2ROperator(bidWindow, bid)
                .addS2ROperator(personWindow, person)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        task.initialize();

        List<DataStream<TestTimestampedRow>> inputStreams = new ArrayList<>();
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
