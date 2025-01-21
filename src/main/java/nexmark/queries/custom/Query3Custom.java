package nexmark.queries.custom;

import nexmark.content.custom.FastAccumulatorFactory;
import nexmark.customdatatypes.custom.Entity;
import nexmark.customdatatypes.tablesaw.TimestampedElement;
import nexmark.operators.r2r.custom.R2Rq2;
import nexmark.operators.r2r.custom.R2Rq3;
import nexmark.operators.r2r.tablesaw.q3.R2Rq3_auction;
import nexmark.operators.r2r.tablesaw.q3.R2Rq3_join;
import nexmark.operators.r2r.tablesaw.q3.R2Rq3_person;
import nexmark.operators.r2s.R2SCustom;
import nexmark.operators.s2r.EvictOnReportWindow;
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
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.sds.SDSDefault;
import relational.stream.RowStream;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query3Custom implements Query {

      /*
        SELECT person.name, person.city,
        person.state, open auction.id
        FROM open auction, person, item
        WHERE open auction.sellerId = person.id
        AND person.state = ‘OR’
        AND open auction.itemid = item.id
        AND item.categoryId = 10;

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
        DataStream<Entity> outStream = new RowStream<>("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(1));

        Report neverReport = new ReportImpl();
        neverReport.add(new Never());

        Time instance = new TimeImpl(0);


        FastAccumulatorFactory accumulateFactory = new FastAccumulatorFactory();

        ContinuousProgram<Entity, Entity, List<Entity>, Entity> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<Entity, Entity, List<Entity>> auctionWindow =
                new EvictOnReportWindow<>(
                        instance,
                        "auctionWindow",
                        accumulateFactory,
                        report);
        StreamToRelationOperator<Entity, Entity, List<Entity>> peopleWindow =
                new UnboundedWindow<>(
                        instance,
                        "peopleWindow",
                        accumulateFactory,
                        neverReport);

        RelationToRelationOperator<List<Entity>> r2r = new R2Rq3(List.of("peopleWindow", "auctionWindow"), "res");

        RelationToStreamOperator<List<Entity>, Entity> r2sOp = new R2SCustom();

        Task<Entity, Entity, List<Entity>, Entity> task = new MyTask<>("1");
        task = task
                .addS2ROperator(peopleWindow, person)
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
