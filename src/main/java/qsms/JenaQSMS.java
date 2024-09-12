package qsms;

import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.rsp4j.api.coordinators.ContinuousProgram;
import org.streamreasoning.rsp4j.api.enums.ReportGrain;
import org.streamreasoning.rsp4j.api.enums.Tick;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.rsp4j.api.operators.r2s.RelationToStreamOperator;
import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.rsp4j.api.querying.Task;
import org.streamreasoning.rsp4j.api.secret.content.ContentFactory;
import org.streamreasoning.rsp4j.api.secret.report.Report;
import org.streamreasoning.rsp4j.api.secret.report.ReportImpl;
import org.streamreasoning.rsp4j.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.rsp4j.api.secret.time.Time;
import org.streamreasoning.rsp4j.api.secret.time.TimeImpl;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import shared.contentimpl.factories.AccumulatorContentFactory;
import shared.coordinators.ContinuousProgramImpl;
import shared.operatorsimpl.r2r.DAG.DAGImpl;
import shared.operatorsimpl.s2r.CSPARQLStreamToRelationOpImpl;
import shared.querying.TaskImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/*
 * This class contains a fully functional example which reads data from two different input streams, applies windowing operators
 * over them and joins them using a "union" operator.
 * In particular, the components used are implementations from the Yasper module, and it uses the RDF-graph data model with Jena APIs.
 *
 * The purpose of this example is to give a general sense of how the system works, more specific examples with
 * custom-defined operators can be found in the same directory.
 *
 */

public class JenaQSMS {

    static String dataFile = JenaQSMS.class.getResource("/dataset_flattened.jsonld").getPath();
    static Model model = RDFDataMgr.loadModel(dataFile);//ModelFactory.createDefaultModel();//

    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input graphs
        QueryStreamGenerator generator = new QueryStreamGenerator();
        // Define input stream objects from the generator
        DataStream<String> inputStreamColors = generator.getStream("http://test/1");
        // define an output stream
        JenaBindingStream outStream = new JenaBindingStream("out");


        /*------------Window Properties------------*/

        // Window properties (report, tick)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());
        Tick tick = Tick.TIME_DRIVEN;
        ReportGrain report_grain = ReportGrain.SINGLE;


        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------Window Content------------*/

        //Entity that represents the window content. In particular, we create an instance that represents an empty content
        QueryOrBindings emptyContent = new QueryOrBindings();

        /*
        Factory used to create the window content. We provide 4 parameters:
        - Function to transform a type I to a type W
        - Function to transform a type W to a type R
        - Function to merge two types R together
        - Object representing the empty content
        The parameter 'I' is the type of the input data (Graph in this case).
        The parameter 'W' is the type of data that we store inside the window (might differ from 'I'), in this case is still Graph.
        The parameter 'R' is the type of data on which we perform our query operations (select, filter, join etc..). We used a custom data type JenaGraphOrBindings

        The logic behind the content can be customized by defining your own factory and content classes, this particular instance
        of content just accumulates what enters the window.
         */
        ContentFactory<String, Op, QueryOrBindings> accumulatorContentFactory = new AccumulatorContentFactory<>(
                s -> Algebra.compile(QueryFactory.create(s)), //this is the S2S
                QueryOrBindings::new,
                (r1, r2) -> new QueryOrBindings(r2, r1),
                emptyContent
        );

        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operators (blueprint of the windows), each with its own size and sliding parameters.
        StreamToRelationOperator<String, Op, QueryOrBindings> s2rOp_one =
                new CSPARQLStreamToRelationOpImpl<>(
                        tick,
                        instance,
                        "w1",
                        accumulatorContentFactory,
                        report_grain,
                        report,
                        5000,
                        5000);

        //Define Relation to Relation operators and chain them together. Here we select all the graphs from the input streams and perform a union

        RelationToRelationOperator<QueryOrBindings> r2rOp1 = new FullGraphJena(model, Collections.singletonList(s2rOp_one.getName()), "partial_1");

        //Relation to Stream operator, used to transform the result of a query (type R) to a stream of output objects (type O)
        RelationToStreamOperator<QueryOrBindings, Binding> r2sOp = new RelationToStreamOpImpl2();


        /*------------Task definition------------*/

        //Define the Tasks, each of which represent a query
        Task<String, Op, QueryOrBindings, Binding> task = new TaskImpl<>();
        task = task
                .addS2ROperator(s2rOp_one, inputStreamColors)
                .addR2ROperator(r2rOp1)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSJenaInv())
                .addTime(instance);
        task.initialize();

        List<DataStream<String>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStreamColors);

        List<DataStream<Binding>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);

        /*------------Continuous Program definition------------*/

        //Define the Continuous Program, which acts as the coordinator of the whole system
        ContinuousProgram<String, Op, QueryOrBindings, Binding> cp = new ContinuousProgramImpl<>();
        cp.buildTask(task, inputStreams, outputStreams);

        /*------------Output Stream consumer------------*/

        outStream.addConsumer((out, el, ts) -> System.out.println(el + " @ " + ts));

        generator.startStreaming();
//        Thread.sleep(20_000);
//        generator.stopStreaming();
    }
}
