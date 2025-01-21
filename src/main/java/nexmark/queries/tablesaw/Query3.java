package nexmark.queries.tablesaw;


import nexmark.customdatatypes.tablesaw.TimestampedElement;
import nexmark.operators.r2r.tablesaw.q3.R2Rq3_auction;
import nexmark.operators.r2r.tablesaw.q3.R2Rq3_join;
import nexmark.operators.r2r.tablesaw.q3.R2Rq3_person;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.EvictOnReportWindow;
import nexmark.operators.s2r.UnboundedWindow;
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

public class Query3 implements Query {

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

        StreamGenerator generator = new StreamGenerator();

        DataStream<TimestampedElement<Table>> auction = generator.getStream("Auction");
        DataStream<TimestampedElement<Table>> bid = generator.getStream("Bid");
        DataStream<TimestampedElement<Table>> person = generator.getStream("Person");

        // define output stream
        DataStream<Row> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(1));

        Report neverReport = new ReportImpl();
        neverReport.add(new Never());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        AccumulatorContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> accumulateFactory = new AccumulatorContentFactory<>(
                (t->t),
                (t->t.getElement().copy()),
                ((t1, t2)->t1.isEmpty()?t2:t1.append(t2)),
                emptyContent
        );

        ContinuousProgram<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> cp = new ContinuousProgramImpl<>();


        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> auctionWindow =
                new EvictOnReportWindow<>(
                        instance,
                        "auctionWindow",
                        accumulateFactory,
                        report);
        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> peopleWindow =
                new UnboundedWindow<>(
                        instance,
                        "peopleWindow",
                        accumulateFactory,
                        neverReport);

        RelationToRelationOperator<Table> r2r_people = new R2Rq3_person(List.of("peopleWindow"), "filteredPeople");
        RelationToRelationOperator<Table> r2r_auction = new R2Rq3_auction(List.of("auctionWindow"), "filteredAuction");
        RelationToRelationOperator<Table> r2r_join = new R2Rq3_join(List.of("filteredPeople", "filteredAuction"), "res");

        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> task = new MyTask<>("1");
        task = task
                .addS2ROperator(peopleWindow, person)
                .addS2ROperator(auctionWindow, auction)
                .addR2ROperator(r2r_people)
                .addR2ROperator(r2r_auction)
                .addR2ROperator(r2r_join)
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
