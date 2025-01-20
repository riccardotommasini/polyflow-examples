package nexmark.queries;

import custom.customoperators.CustomTumblingWindow;
import nexmark.content.MaxContentFactory;
import nexmark.customdatatypes.TimestampedElement;
import nexmark.operators.r2r.tablesaw.R2Rq7;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.stream.StreamGenerator;
import nexmark.utils.Query;
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
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import nexmark.utils.MyTask;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query7 implements Query {

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
        report.add(new OnWindowClose());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        MaxContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> contentFactory = new MaxContentFactory<>(
                (t->t),
                (t->t.getElement().copy()),
                (t1, t2)->{
                    if(t1 == null)
                        return -1;
                    Long currMax, element;
                    currMax = t1.getElement().longColumn("price").get(0);
                    element = t2.getElement().longColumn("price").get(0);
                    if(currMax < element){
                        return -1;
                    }
                    else if(currMax > element){
                        return 1;
                    }
                    else return 0;
                },
                emptyContent);

        ContinuousProgram<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> cp = new ContinuousProgramImpl<>();



        //only interested in the bidWindow, which is a tumbling window of size 10 minutes
        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> bidWindow =
                new CustomTumblingWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report,
                        100); // width of 1000 is too much given the timestamps generated in our file

        RelationToRelationOperator<Table> r2r = new R2Rq7(List.of("bidWindow"), "res");
        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> task = new MyTask<>("1");
        task = task
                .addS2ROperator(bidWindow, bid)
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
