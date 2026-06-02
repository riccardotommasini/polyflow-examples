package relational.examples;

import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.enums.Tick;
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
import static tech.tablesaw.aggregate.AggregateFunctions.*;

import org.streamreasoning.polyflow.base.contentimpl.factories.AggregateContentFactory;
import org.streamreasoning.polyflow.base.contentimpl.factories.ContainerContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.MBHoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.state.MapMultiBufferState;
import org.streamreasoning.polyflow.base.processing.ParallelContinuousProgram;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import tech.tablesaw.api.*;

import java.util.ArrayList;
import java.util.List;

public class keyval_partition{
    public static void main(String[] args) throws InterruptedException {

        RowStreamGenerator generator = new RowStreamGenerator();

        DataStream<Tuple> inputStream_1 = generator.getStream("http://test/stream1");
        // define output stream
        DataStream<Tuple> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        Tick tick = Tick.TIME_DRIVEN;
        Time instance_1 = new TimeImpl(0);
        Time instance_2 = new TimeImpl(0);
        Time instance_3 = new TimeImpl(0);


        Table emptyContent = Table.create();

        AggregateContentFactory<Tuple, Tuple, Table> aggregateContentFactory = new AggregateContentFactory<>(
                t -> t,
                (t) -> {
                    Table r = Table.create();

                    for (int i = 0; i < t.getSize(); i++) {
                        if (t.getValue(i) instanceof Long) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                LongColumn lc = LongColumn.create(columnName);
                                lc.append((Long) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                LongColumn lc = (LongColumn) r.column(columnName);
                                lc.append((Long) t.getValue(i));
                            }

                        } else if (t.getValue(i) instanceof Integer) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                IntColumn lc = IntColumn.create(columnName);
                                lc.append((Integer) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                IntColumn lc = (IntColumn) r.column(columnName);
                                lc.append((Integer) t.getValue(i));
                            }
                        } else if (t.getValue(i) instanceof Boolean) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                BooleanColumn lc = BooleanColumn.create(columnName);
                                lc.append((Boolean) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                BooleanColumn lc = (BooleanColumn) r.column(columnName);
                                lc.append((Boolean) t.getValue(i));
                            }
                        } else if (t.getValue(i) instanceof String) {
                            String columnName = "c" + (i + 1);
                            if (!r.containsColumn(columnName)) {
                                StringColumn lc = StringColumn.create(columnName);
                                lc.append((String) t.getValue(i));
                                r.addColumns(lc);
                            } else {
                                StringColumn lc = (StringColumn) r.column(columnName);
                                lc.append((String) t.getValue(i));
                            }
                        }
                    }
                    return r;
                },
                (r1, r2) -> r1.isEmpty() ? r2 : r1.append(r2),
                (table) -> table.summarize("c3", mean, max, min).by("c1"),
                emptyContent
        );

        Table emptyContent2 = Table.create();
        ContainerContentFactory<Tuple, Tuple, Table, Long> containerContentFactory = new ContainerContentFactory<>(
                i-> (Long)i.getValue(0),
                w->null,
                r->null,
                (t1, t2)-> t1.isEmpty()?t2: t1.append(t2),
                emptyContent2,
                aggregateContentFactory);


        StreamToRelationOperator<Tuple, Table> s2rOp_1 =
                new MBHoppingWindowOpImpl<>(
                        tick,
                        instance_1,
                        "w1",
                        new MapMultiBufferState<>(containerContentFactory),
                        report,
                        1000,
                        1000);
        StreamToRelationOperator<Tuple, Table> s2rOp_2 =
                new MBHoppingWindowOpImpl<>(
                        tick,
                        instance_2,
                        "w1",
                        new MapMultiBufferState<>(containerContentFactory),
                        report,
                        1000,
                        1000);
        StreamToRelationOperator<Tuple, Table> s2rOp_3 =
                new MBHoppingWindowOpImpl<>(
                        tick,
                        instance_3,
                        "w1",
                        new MapMultiBufferState<>(containerContentFactory),
                        report,
                        1000,
                        1000);


        RelationToStreamOperator<Table, Tuple> r2sOp = new RelationToStreamjtablesawImpl();

        Task<Tuple, Tuple, Table, Tuple> task_1 = new TaskImpl<>("1");
        task_1 = task_1.addS2ROperator(s2rOp_1, inputStream_1)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_1);
        task_1.initialize();

        Task<Tuple, Tuple, Table, Tuple> task_2 = new TaskImpl<>("2");
        task_2 = task_2.addS2ROperator(s2rOp_2, inputStream_1)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_2);
        task_2.initialize();

        Task<Tuple, Tuple, Table, Tuple> task_3 = new TaskImpl<>("3");
        task_3 = task_3.addS2ROperator(s2rOp_3, inputStream_1)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance_3);
        task_3.initialize();

        List<DataStream<Tuple>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStream_1);


        List<DataStream<Tuple>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ParallelContinuousProgram<>(i-> (Long)i.getValue(0), 3);

        cp.buildTask(task_1, inputStreams, outputStreams);
        cp.buildTask(task_2, inputStreams, outputStreams);
        cp.buildTask(task_3, inputStreams, outputStreams);



        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();

    }
}
