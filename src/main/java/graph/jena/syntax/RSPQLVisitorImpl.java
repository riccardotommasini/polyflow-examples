package graph.jena.syntax;

import graph.ContinuousQuery;
import graph.jena.datatypes.JenaGraphOrBindings;
import graph.jena.operatorsimpl.r2r.jena.FullQueryUnaryJena;
import graph.jena.operatorsimpl.r2s.RelationToStreamOpImpl;
import graph.jena.sds.SDSJena;
import graph.jena.stream.JenaBindingStream;
import graph.jena.stream.JenaRDFStream;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.jena.graph.Graph;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.engine.binding.Binding;
import org.streamreasoning.polyflow.api.enums.Tick;
import org.streamreasoning.polyflow.api.operators.r2r.RelationToRelationOperator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.assigner.StreamToRelationOperator;
import org.streamreasoning.polyflow.api.processing.Task;
import org.streamreasoning.polyflow.api.secret.report.Report;
import org.streamreasoning.polyflow.api.secret.report.ReportImpl;
import org.streamreasoning.polyflow.api.secret.report.strategies.OnWindowClose;
import org.streamreasoning.polyflow.api.secret.time.Time;
import org.streamreasoning.polyflow.api.secret.time.TimeImpl;
import org.streamreasoning.polyflow.api.stream.data.DataStream;
import org.streamreasoning.polyflow.base.contentimpl.factories.AccumulatorContentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.dag.DAGImpl;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.HoppingWindowOpImpl;
import org.streamreasoning.polyflow.base.processing.TaskImpl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class RSPQLVisitorImpl extends RSPQLBaseVisitor<Task<Graph, Graph, JenaGraphOrBindings, Binding>> {

    private final Task<Graph, Graph, JenaGraphOrBindings, Binding> task;
    private final List<DataStream<Graph>> inputStreams = new ArrayList<>();
    private final List<DataStream<Binding>> outputStreams = new ArrayList<>();
    private final PrefixMap prefixMap = new PrefixMap();

    private final Report report = new ReportImpl();
    private final Tick tick = Tick.TIME_DRIVEN;
    private final Time instance = new TimeImpl(0);

    private final AccumulatorContentFactory<Graph, Graph, JenaGraphOrBindings> accumulatorContentFactory = new AccumulatorContentFactory<>(
            (g) -> g,
            JenaGraphOrBindings::new,
            JenaGraphOrBindings::new,
            new JenaGraphOrBindings()
    );

    private String select;
    private List<String> resultVars;
    private Query sparql;
    private List<String> windows = new ArrayList<>();

    public RSPQLVisitorImpl() {
        this.task = new TaskImpl<>("1");
        report.add(new OnWindowClose());
        this.task.addDAG(new DAGImpl<>()).addSDS(new SDSJena()).addTime(instance);
    }

    @Override
    public Task<Graph, Graph, JenaGraphOrBindings, Binding> visitNamedWindowClause(RSPQLParser.NamedWindowClauseContext ctx) {
        String streamUri = prefixMap.expandIfPrefixed(RDFUtils.trimTags(ctx.streamUri().getText()));
        JenaRDFStream s = new JenaRDFStream(streamUri);
        inputStreams.add(s);

        String windowUri = prefixMap.expandIfPrefixed(RDFUtils.trimTags(ctx.windowUri().getText()));
        RSPQLParser.LogicalWindowContext c = ctx.window().logicalWindow();
        Duration range = Duration.parse(c.logicalRange().duration().getText());
        Duration step = c.logicalStep() != null ? Duration.parse(c.logicalStep().duration().getText()) : range;

        System.out.println(windowUri);

        StreamToRelationOperator<Graph, Graph, JenaGraphOrBindings> s2rOp =
                new HoppingWindowOpImpl<>(
                        tick,
                        instance,
                        windowUri,
                        accumulatorContentFactory,
                        report,
                        range.toMillis(),
                        step.toMillis());

        task.addS2ROperator(s2rOp, s);
        windows.add(windowUri);

        return super.visitNamedWindowClause(ctx);
    }

    @Override
    public Task<Graph, Graph, JenaGraphOrBindings, Binding> visitSelectClause(RSPQLParser.SelectClauseContext ctx) {
        this.select = ctx.getText();
        return super.visitSelectClause(ctx);
    }

    @Override
    public Task<Graph, Graph, JenaGraphOrBindings, Binding> visitWhereClause(RSPQLParser.WhereClauseContext ctx) {
        String where = ctx.getText();
        where = where.replace("WINDOW", "GRAPH");
        this.sparql = QueryFactory.create(select + "\n" + where);
        this.resultVars = sparql.getResultVars();
        RelationToRelationOperator<JenaGraphOrBindings> r2rOp1 = new FullQueryUnaryJena(sparql, windows, "partial_1");

        task.addR2ROperator(r2rOp1);

        return super.visitWhereClause(ctx);
    }

    @Override
    public Task<Graph, Graph, JenaGraphOrBindings, Binding> visitPrefixDecl(RSPQLParser.PrefixDeclContext ctx) {
        prefixMap.addPrefix(ctx.PNAME_NS().getText(), RDFUtils.trimTags(ctx.IRIREF().getText()));
        return super.visitPrefixDecl(ctx);
    }

    @Override
    public Task<Graph, Graph, JenaGraphOrBindings, Binding> visitOutputStream(RSPQLParser.OutputStreamContext ctx) {
        RSPQLParser.SourceSelectorContext sourceSelectorContext = ctx.sourceSelector();
        RSPQLParser.IriContext iri1 = sourceSelectorContext.iri();
        TerminalNode iriref = iri1.IRIREF();
        String text = "";
        if (iriref == null) {
            text = iri1.prefixedName().getText();
        } else {
            text = iriref.getText();
            text = RDFUtils.trimTags(text);
        }
        outputStreams.add(new JenaBindingStream(text));
        task.addR2SOperator(new RelationToStreamOpImpl());
        return super.visitOutputStream(ctx);
    }


    public ContinuousQuery<Graph, Graph, JenaGraphOrBindings, Binding> getQuery() {
        this.task.initialize();
        return new ContinuousQuery<>(outstreams().get(0).getName(), task, resultVars, outputStreams.get(0), inputStreams);
    }

    public List<DataStream<Graph>> instreams() {
        return inputStreams;
    }

    public List<DataStream<Binding>> outstreams() {
        return outputStreams;
    }

    public Query getSPARQLQuery() {
        return sparql;
    }

    public List<String> getResultVars() {
        return resultVars;
    }

}