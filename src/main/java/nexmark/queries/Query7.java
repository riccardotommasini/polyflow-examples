package nexmark.queries;

import custom.customoperators.CustomTumblingWindow;
import nexmark.content.LogicalSlidingContentFactory;
import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.operators.r2r.R2Rq7;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.LogicalSlidingWindow;
import nexmark.report.Periodic;
import nexmark.stream.StreamGenerator;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.content.ContentFactory;
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

public class Query7 {

        /*
        Query 7 monitors the highest price items currently
        on auction. Every ten minutes, this query returns the
        highest bid (and associated itemid) in the most re-
        cent ten minutes. This query uses a time-based, fixed-
        window group by. The syntax FIXEDRANGE is used
        in place of RANGE to indicate that the highest bid
        should be evaluated every ten minutes instead of over
        a sliding ten minute window.
        SELECT bid.price, bid.itemid
        FROM bid where bid.price =
        (SELECT MAX(bid.price)
        FROM bid [FIXEDRANGE
        10 MINUTES PRECEDING]);
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
        report.add(new OnWindowClose());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        AccumulatorContentFactory<TestTimestampedRow, TestTimestampedRow, Table> contentFactory = new AccumulatorContentFactory<>(
                (t->t),
                (t->t.getRow().copy()),
                ((t1, t2)->t1.isEmpty()?t2:t1.append(t2)),
                emptyContent);

        ContinuousProgram<TestTimestampedRow, TestTimestampedRow, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> auctionWindow =
                new LogicalSlidingWindow<>(
                        instance,
                        "auctionWindow",
                        contentFactory,
                        report,
                        500);

        //only interested in the bidWindow, which is a tumbling window of size 10 minutes
        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> bidWindow =
                new CustomTumblingWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report,
                        500);


        RelationToRelationOperator<Table> r2r = new R2Rq7(List.of("bidWindow"), "res");
        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TestTimestampedRow, TestTimestampedRow, Table, Row> task = new TaskImpl<>();
        task = task.addS2ROperator(auctionWindow, auction)
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
