package nexmark.queries;

import nexmark.content.LogicalSlidingContentFactory;
import nexmark.content.MaxContentFactory;
import nexmark.content.PhysicalSlidingContent;
import nexmark.content.PhysicalSlidingContentFactory;
import nexmark.customdatatypes.TimestampedElement;
import nexmark.operators.r2r.R2Rq4;
import nexmark.operators.r2r.R2Rq6;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.LogicalSlidingWindow;
import nexmark.operators.s2r.PhysicalSlidingWindow;
import nexmark.operators.s2r.UnboundedWindow;
import nexmark.report.Always;
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
import org.streamreasoning.polyflow.base.contentimpl.factories.ContainerContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.ArrayList;
import java.util.List;

public class Query6 implements Query {

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

Assumption: no bids arrive for a closed auction. We don not use the closing price of an auction, but the closing price
up until that point (basically, every report gives you a snapshot of the possible closing price if the auction were to end in that isntant)

        */
    //TODO creare un content per expired auction + physical sliding

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
        report.add(new Always());

        Report neverReport = new ReportImpl();
        neverReport.add(new Never());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();


        PhysicalSlidingContentFactory<Table, Table> slidingFactory = new PhysicalSlidingContentFactory<>(
                emptyContent,
                10,
                (t->t.getElement().copy()),
                ((t1, t2)->t1.isEmpty()?t2:t1.append(t2))
        );

        MaxContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table> maxContentFactory = new MaxContentFactory<>(
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
                emptyContent
        );

        ContainerContentFactory<TimestampedElement<Table>, TimestampedElement<Table>, Table, Long> containerContentFactory = new ContainerContentFactory<>(
                i-> i.getElement().longColumn("auction").get(0),
                w->null,
                r->null,
                (t1, t2)-> t1.isEmpty()?t2: t1.append(t2),
                emptyContent,
                maxContentFactory);

        ContinuousProgram<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> cp = new ContinuousProgramImpl<>();

        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> auctionWindow =
                new PhysicalSlidingWindow<>(
                        instance,
                        "auctionWindow",
                        slidingFactory, //TODO: rivedere anche qui come nella q4 se riusciamo a tirare fuori le closed auctions
                        report
                        );

        StreamToRelationOperator<TimestampedElement<Table>, TimestampedElement<Table>, Table> bidWindow =
                new UnboundedWindow<>(
                        instance,
                        "bidWindow",
                        containerContentFactory,
                        neverReport);

        RelationToRelationOperator<Table> r2r = new R2Rq6(List.of("auctionWindow", "bidWindow"), "res");

        RelationToStreamOperator<Table, Row> r2sOp = new RelationToStreamRow();

        Task<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> task = new TaskImpl<>("1");
        task = task
                .addS2ROperator(bidWindow, bid)
                .addS2ROperator(auctionWindow, auction)
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
