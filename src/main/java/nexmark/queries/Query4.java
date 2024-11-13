package nexmark.queries;


import nexmark.customdatatypes.TimestampedElement;
import nexmark.operators.r2r.q3.R2Rq3_auction;
import nexmark.operators.r2r.q3.R2Rq3_join;
import nexmark.operators.r2r.q3.R2Rq3_person;
import nexmark.operators.r2s.RelationToStreamRow;
import nexmark.operators.s2r.EvictOnReportWindow;
import nexmark.operators.s2r.UnboundedWindow;
import nexmark.report.Never;
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

public class Query4 {

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

        */
//USARE KEY VALUE WINDOW PER LE BID CON UN MAXCONTENT CHE SI SALVA SEMPRE LA BID MASSIMA. REPORTARE QUANDO
    //IL MASSIMO CAMBIA (USARE UNO STATEFUL CONTENT PER DIRE ALLA WINDOW CHE BISOGNA REPORTARE).
    //EVICTARE LE AUCTION QUANDO SCADONO

    public static void main(String[] args) throws InterruptedException {

        StreamGenerator generator = new StreamGenerator();

        DataStream<TimestampedElement<Table>> auction = generator.getStream("Auction");
        DataStream<TimestampedElement<Table>> bid = generator.getStream("Bid");
        DataStream<TimestampedElement<Table>> person = generator.getStream("Person");

        // define output stream
        DataStream<Row> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new Periodic(500));

        Report neverReport = new ReportImpl();
        neverReport.add(new Never());

        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();
        //The sliding factor should be the same as the window size

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

        Task<TimestampedElement<Table>, TimestampedElement<Table>, Table, Row> task = new TaskImpl<>();
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

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();

    }


}
