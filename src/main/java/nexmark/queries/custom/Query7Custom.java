package nexmark.queries.custom;

import custom.customoperators.CustomTumblingWindow;
import nexmark.content.custom.FastMaxFactory;
import nexmark.operators.r2r.custom.R2Rq7;
import nexmark.operators.r2s.R2SCustom;
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
import org.streamreasoning.polyflow.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;
import relational.stream.RowStream;
import tech.tablesaw.api.Table;

import nexmark.customdatatypes.custom.Entity;
import java.util.ArrayList;
import java.util.List;

public class Query7Custom implements Query {

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

        StreamGeneratorCustom generator = new StreamGeneratorCustom();

        DataStream<Entity> auction = generator.getStream("Auction");
        DataStream<Entity> bid = generator.getStream("Bid");
        DataStream<Entity> person = generator.getStream("Person");

        // define output stream
        DataStream<Entity> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        FastMaxFactory contentFactory = new FastMaxFactory();

        ContinuousProgram<Entity, Entity, List<Entity>, Entity> cp = new ContinuousProgramImpl<>();



        //only interested in the bidWindow, which is a tumbling window of size 10 minutes
        StreamToRelationOperator<Entity, Entity, List<Entity>> bidWindow =
                new CustomTumblingWindow<>(
                        instance,
                        "bidWindow",
                        contentFactory,
                        report,
                        100); // width of 1000 is too much given the timestamps generated in our file

        RelationToRelationOperator<List<Entity>> r2r = new R2Rq7(List.of("bidWindow"), "res");
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

