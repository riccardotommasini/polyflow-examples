package nexmark.queries.custom;

import nexmark.content.ExpiredAuctionContentFactory;
import nexmark.content.MaxContentFactory;
import nexmark.content.custom.EvictContainerContent;
import nexmark.content.custom.EvictContainerFactory;
import nexmark.content.custom.FastMaxFactory;
import nexmark.content.custom.Q4Factory;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.tablesaw.TimestampedElement;
import nexmark.operators.r2r.custom.R2Rq4;
import nexmark.operators.r2s.R2SCustom;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.UnboundedWindow;
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
import org.streamreasoning.polyflow.base.contentimpl.factories.ContainerContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query4Custom implements Query {

      /*
        Query 4 joins the category file
        with the closed auction stream to calculate average
        closing price for each. The query should output up-
        dated prices when new closing prices arrive for a par-
        ticular group.
        SELECT C.id, AVG(CA.price)
        FROM category C, item I, closed auction CA
        WHERE C.id = I.categoryId
        AND I.id = CA.itemid
        GROUP BY C.id;

        Assumption: no bids arrive for a closed auction.
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
        report.add(new Periodic(1));

        Report neverReport = new ReportImpl();
        neverReport.add(new Never());

        Time instance = new TimeImpl(0);



        FastMaxFactory maxContentFactory = new FastMaxFactory();

        EvictContainerFactory containerContentFactory = new EvictContainerFactory(maxContentFactory);
        Q4Factory expiredAuctionContentFactory = new Q4Factory((EvictContainerContent) containerContentFactory.create());

        ContinuousProgram<Entity, Entity, List<Entity>, Entity> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<Entity, Entity, List<Entity>> auctionWindow =
                new UnboundedWindow<>(
                        instance,
                        "auctionWindow",
                        expiredAuctionContentFactory,
                        report);

        StreamToRelationOperator<Entity, Entity, List<Entity>> bidWindow =
                new UnboundedWindow<>(
                        instance,
                        "bidWindow",
                        containerContentFactory,
                        neverReport);

        RelationToRelationOperator<List<Entity>> r2r = new R2Rq4(List.of("auctionWindow", "bidWindow"), "res");

        RelationToStreamOperator<List<Entity>, Entity> r2sOp = new R2SCustom();

        Task<Entity, Entity, List<Entity>, Entity> task = new MyTask<>("1");
        task = task
                .addS2ROperator(bidWindow, bid)
                .addS2ROperator(auctionWindow, auction)
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
