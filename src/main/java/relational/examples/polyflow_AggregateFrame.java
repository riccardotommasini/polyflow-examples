package relational.examples;

import org.javatuples.Tuple;
import org.streamreasoning.polyflow.api.processing.ContinuousProgram;
import org.streamreasoning.polyflow.base.processing.ContinuousProgramImpl;
import org.streamreasoning.polyflow.api.enums.AggregationFunction;
import org.streamreasoning.polyflow.api.enums.FrameClosingCondition;
import org.streamreasoning.polyflow.api.enums.FrameType;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.base.processing.TaskImpl;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnStateReady;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import relational.operatorsimpl.r2s.RelationToStreamjtablesawImpl;
import relational.sds.SDSjtablesaw;
import relational.stream.RowStream;
import relational.stream.RowStreamGenerator;
import org.streamreasoning.polyflow.base.contentimpl.factories.StatefulContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.SBFramesWindowOpImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.state.SingleBufferState;
import tech.tablesaw.api.*;

import java.util.ArrayList;
import java.util.List;

public class polyflow_AggregateFrame {

    public static void main(String [] args) throws InterruptedException {

        RowStreamGenerator generator = new RowStreamGenerator();

        DataStream<Tuple> inputStream_1 = generator.getStream("http://test/stream1");
        // define output stream
        DataStream<Tuple> outStream = new RowStream("out");

        // Engine properties
        Report report = new ReportImpl();
        report.add(new OnStateReady());

        Tick tick = Tick.TIME_DRIVEN;
        Time instance = new TimeImpl(0);
        Table emptyContent = Table.create();

        StatefulContentFactory<Tuple, Tuple, Table> statefulContentFactory = new StatefulContentFactory<>(
                ()-> 0,
                (t, s)-> (Integer)s + (Integer)t.getValue(2),
                (s)->(Integer)s >20,
                t->t,
                (t)->{
                    Table r = Table.create();

                    for(int i = 0; i<t.getSize(); i++){
                        if(t.getValue(i) instanceof Long){
                            String columnName = "c"+ (i+1);
                            if(!r.containsColumn(columnName)) {
                                LongColumn lc = LongColumn.create(columnName);
                                lc.append((Long) t.getValue(i));
                                r.addColumns(lc);
                            }
                            else{
                                LongColumn lc = (LongColumn) r.column(columnName);
                                lc.append((Long) t.getValue(i));
                            }

                        }
                        else if(t.getValue(i) instanceof Integer){
                            String columnName = "c"+ (i+1);
                            if(!r.containsColumn(columnName)) {
                                IntColumn lc = IntColumn.create(columnName);
                                lc.append((Integer) t.getValue(i));
                                r.addColumns(lc);
                            }
                            else{
                                IntColumn lc = (IntColumn) r.column(columnName);
                                lc.append((Integer) t.getValue(i));
                            }
                        }
                        else if(t.getValue(i) instanceof Boolean){
                            String columnName = "c"+ (i+1);
                            if(!r.containsColumn(columnName)) {
                                BooleanColumn lc = BooleanColumn.create(columnName);
                                lc.append((Boolean) t.getValue(i));
                                r.addColumns(lc);
                            }
                            else{
                                BooleanColumn lc = (BooleanColumn) r.column(columnName);
                                lc.append((Boolean) t.getValue(i));
                            }
                        }
                        else if(t.getValue(i) instanceof String){
                            String columnName = "c"+ (i+1);
                            if(!r.containsColumn(columnName)) {
                                StringColumn lc = StringColumn.create(columnName);
                                lc.append((String) t.getValue(i));
                                r.addColumns(lc);
                            }
                            else{
                                StringColumn lc = (StringColumn) r.column(columnName);
                                lc.append((String) t.getValue(i));
                            }
                        }
                    }
                    return r;
                },
                (r1, r2)->r1.isEmpty()? r2:r1.append(r2),
                emptyContent

        );


        ContinuousProgram<Tuple, Tuple, Table, Tuple> cp = new ContinuousProgramImpl<>();

        StreamToRelationOperator<Tuple, Table> s2rOp_1 =
                new SBFramesWindowOpImpl<>(
                        tick,
                        instance,
                        "w1",
                        new SingleBufferState<>(statefulContentFactory, (tuple, timestamp) -> true, (tuple, timestamp) -> true),
                        report,
                        FrameType.AGGREGATE,
                        20,
                        AggregationFunction.SUM,
                        tuple -> ((Number) tuple.getValue(2)).doubleValue(),
                        FrameClosingCondition.GREATER_THAN);




        RelationToStreamOperator<Table, Tuple> r2sOp = new RelationToStreamjtablesawImpl();

        Task<Tuple, Tuple, Table, Tuple> task = new TaskImpl<>("1");
        task = task.addS2ROperator(s2rOp_1, inputStream_1)
                .addR2SOperator(r2sOp)
                .addSDS(new SDSjtablesaw())
                .addDAG(new DAGImpl<>())
                .addTime(instance);
        task.initialize();

        List<DataStream<Tuple>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStream_1);


        List<DataStream<Tuple>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        cp.buildTask(task, inputStreams, outputStreams);

        outStream.addConsumer((out, el, ts)-> System.out.println(el + " @ " + ts));

        generator.startStreaming();

    }

}
