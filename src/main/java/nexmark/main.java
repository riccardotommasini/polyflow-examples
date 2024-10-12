package nexmark;

import nexmark.content.SlidingContentFactory;
import nexmark.customdatatypes.TestTimestampedRow;
import nexmark.operators.RelationToStreamRow;
import nexmark.operators.SlidingWindow;
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
import tech.tablesaw.api.*;

import java.util.ArrayList;
import java.util.List;

public class main {

    public static void main(String[] args) throws InterruptedException {

        StreamGenerator generator = new StreamGenerator();

        DataStream<TestTimestampedRow> auction = generator.getStream("Auction");
        DataStream<TestTimestampedRow> bid = generator.getStream("Bid");
        DataStream<TestTimestampedRow> person = generator.getStream("Person");

        // define output stream
        DataStream<Row> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        SlidingContentFactory contentFactory = new SlidingContentFactory(emptyContent, 500);

        ContinuousProgram<TestTimestampedRow, TestTimestampedRow, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> auctionWindow =
                new SlidingWindow<>(
                        instance,
                        "auctionWindow",
                        contentFactory,
                        report,
                        500);
        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> bidWindow =
                new SlidingWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report,
                        500);
        StreamToRelationOperator<TestTimestampedRow, TestTimestampedRow, Table> personWindow =
                new SlidingWindow<>(
                        instance,
                        "personWindow",
                        contentFactory,
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
