package LLM;

import LLM.customdatatypes.ImageDescription_scenegraph;
import LLM.customoperators.CustomTumblingWindow;
import LLM.customoperators.LLMOperator_scenegraph;
import org.streamreasoning.rsp4j.api.coordinators.ContinuousProgram;
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
import shared.querying.TaskImpl;
import shared.sds.SDSDefault;
import LLM.stream.ImageStream;
import LLM.stream.ImageStreamGenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class example_scenegraph {
    public static void main(String[] args) throws InterruptedException {

        /*------------Input and Output Stream definitions------------*/

        // Define a generator to create input elements
        ImageStreamGenerator generator = new ImageStreamGenerator();

        // Define an input stream
        DataStream<String> inputStream= generator.getStream("input");

        // define an output stream
        DataStream<String> outStream = new ImageStream("output");

        /*------------Window Content------------*/

        //Entity that represents a neutral element for our operations on the 'R' data type
        ImageDescription_scenegraph emptyImg = new ImageDescription_scenegraph();

        // Factory object to manage the window content, more informations on our GitHub guide!
        ContentFactory<String, String, ImageDescription_scenegraph> accumulatorContentFactory = new AccumulatorContentFactory<>(
                (url) -> url,
                (url) -> {
                    ImageDescription_scenegraph desc = new ImageDescription_scenegraph();
                    desc.addUrl(url);
                    return desc;
                },
                (img1, img2) -> {
                    if(img1.getImages().size()>img2.getImages().size()){
                        img1.addAll(img2);
                        return img1;
                    }
                    else{
                        img2.addAll(img1);
                        return img2;
                    }
                },
                emptyImg
        );


        /*------------Window Properties------------*/

        // Window properties (report)
        Report report = new ReportImpl();
        report.add(new OnWindowClose());

        //Time object used to represent the time in our application
        Time instance = new TimeImpl(0);


        /*------------S2R, R2R and R2S Operators------------*/

        //Define the Stream to Relation operator (blueprint of the windows)
        StreamToRelationOperator<String, String, ImageDescription_scenegraph> s2r_op =
                new CustomTumblingWindow<>(
                        instance,
                        "TumblingWindow",
                        accumulatorContentFactory,
                        report,
                        1000);

        //Define Relation to Relation operators and chain them together. Here we filter out fruits that are underripe
        RelationToRelationOperator<ImageDescription_scenegraph> r2r_llm = new LLMOperator_scenegraph("Give me the scene graphs of these pictures in JSON format. " +
                "Give me only the graphs, do not add words to the answer and do not include the formatting type at the beginning." +
                "Moreover, the scene graph must include only shapes, color and type.", Collections.singletonList(s2r_op.getName()), "scene_graphs");

        //Relation to Stream operator, take the final fruit basket and send out each fruit
        RelationToStreamOperator<ImageDescription_scenegraph, String> r2sOp = new RelationToStreamOperator<>() {};


        /*------------Task definition------------*/

        //Define the Tasks, each of which represent a query
        Task<String, String, ImageDescription_scenegraph, String> task = new TaskImpl<>();
        task = task.addS2ROperator(s2r_op, inputStream)
                .addR2ROperator(r2r_llm)
                .addR2SOperator(r2sOp)
                .addDAG(new DAGImpl<>())
                .addSDS(new SDSDefault<>())
                .addTime(instance);
        task.initialize();




        /*------------Continuous Program definition------------*/

        //Define the Continuous Program, which acts as the coordinator of the whole system
        ContinuousProgram<String, String, ImageDescription_scenegraph, String> cp = new ContinuousProgramImpl<>();

        List<DataStream<String>> inputStreams = new ArrayList<>();
        inputStreams.add(inputStream);

        List<DataStream<String>> outputStreams = new ArrayList<>();
        outputStreams.add(outStream);


        cp.buildTask(task, inputStreams, outputStreams);


        /*------------Output Stream consumer------------*/

        outStream.addConsumer((out, el, ts) -> System.out.println("Output Element: ["+el+ "]" + " @ " + ts));

        generator.startStreaming();
        Thread.sleep(120_000);
        generator.stopStreaming();
    }
}
