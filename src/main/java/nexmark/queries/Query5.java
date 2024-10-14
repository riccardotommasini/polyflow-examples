package nexmark.queries;

import nexmark.content.LogicalSlidingContentFactory;
import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.operators.r2r.R2Rq5;
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

public class Query5 {

        /*
        This query selects the item with the most bids in
        the past one hour time period; the “hottest” item.
        The results are output every minute. This query uses
        a time-based, sliding window group by operation.
        SELECT bid.itemid
        FROM bid [RANGE 60 MINUTES PRECEDING]
        WHERE (SELECT COUNT(bid.itemid)
        FROM bid [PARTITION BY bid.itemid
        RANGE 60 MINUTES PRECEDING])
        >= ALL (SELECT COUNT(bid.itemid)
        FROM bid [PARTITION BY bid.itemid
        RANGE 60 MINUTES PRECEDING]
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
        //The sliding factor should be the same as the window size
        LogicalSlidingContentFactory contentFactory = new LogicalSlidingContentFactory(emptyContent, 1000);

        ContinuousProgram<TestTimestampedRow, TestTimestampedRow, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> bidWindow =
                new LogicalSlidingWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report,
                        1000);


        RelationToRelationOperator<Table> r2r = new R2Rq5(List.of("bidWindow"), "res");

        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TestTimestampedRow, TestTimestampedRow, Table, Row> task = new TaskImpl<>();
        task = task
                .addS2ROperator(bidWindow, bid)
                .addR2ROperator(r2r)
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
