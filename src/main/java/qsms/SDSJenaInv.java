package qsms;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.Context;
import org.streamreasoning.rsp4j.api.sds.SDS;
import org.streamreasoning.rsp4j.api.sds.timevarying.TimeVarying;

import java.util.*;
import java.util.stream.Stream;

public class SDSJenaInv implements SDS<QueryOrBindings>, Dataset {
    private final Set<TimeVarying<QueryOrBindings>> defs = new HashSet();
    private final Map<Node, TimeVarying<QueryOrBindings>> tvgs = new HashMap();
    private final Node def = NodeFactory.createURI("def");
    private static Dataset dataset = DatasetFactory.createGeneral();

    public SDSJenaInv() {
    }

    public Collection<TimeVarying<QueryOrBindings>> asTimeVaryingEs() {
        return this.tvgs.values();
    }

    public void add(String iri, TimeVarying<QueryOrBindings> tvg) {
        this.tvgs.put(NodeFactory.createURI(iri), tvg);
    }

    public void add(TimeVarying<QueryOrBindings> tvg) {
        this.defs.add(tvg);
    }

    public SDS<QueryOrBindings> materialize(long ts) {

        //todo this method produces a jointly optimised query considering all the queries in window
        DatasetGraph dg = dataset.asDatasetGraph();

        this.defs.forEach((g) -> {
            g.materialize(ts);
        });

        this.tvgs.entrySet().forEach((e) -> {
            e.getValue().materialize(ts);
        });

        return this;
    }

    public Stream<QueryOrBindings> toStream() {
        return null;
    }

    public Model getDefaultModel() {
        return this.getDefaultModel();
    }

    public Model getUnionModel() {
        return dataset.getUnionModel();
    }

    public Dataset setDefaultModel(Model model) {
        return dataset.setDefaultModel(model);
    }

    public Model getNamedModel(String uri) {
        return dataset.getNamedModel(uri);
    }

    public Model getNamedModel(Resource uri) {
        return dataset.getNamedModel(uri);
    }

    public boolean containsNamedModel(String uri) {
        return dataset.containsNamedModel(uri);
    }

    public boolean containsNamedModel(Resource uri) {
        return dataset.containsNamedModel(uri);
    }

    public Dataset addNamedModel(String uri, Model model) {
        return dataset.addNamedModel(uri, model);
    }

    public Dataset addNamedModel(Resource resource, Model model) {
        return dataset.addNamedModel(resource, model);
    }

    public Dataset removeNamedModel(String uri) {
        return dataset.removeNamedModel(uri);
    }

    public Dataset removeNamedModel(Resource resource) {
        return dataset.removeNamedModel(resource);
    }

    public Dataset replaceNamedModel(String uri, Model model) {
        return dataset.replaceNamedModel(uri, model);
    }

    public Dataset replaceNamedModel(Resource resource, Model model) {
        return dataset.replaceNamedModel(resource, model);
    }

    public Iterator<String> listNames() {
        return dataset.listNames();
    }

    public Iterator<Resource> listModelNames() {
        return dataset.listModelNames();
    }

    public Lock getLock() {
        return dataset.getLock();
    }

    public Context getContext() {
        return dataset.getContext();
    }

    public boolean supportsTransactions() {
        return dataset.supportsTransactions();
    }

    public boolean supportsTransactionAbort() {
        return dataset.supportsTransactionAbort();
    }

    public void begin(TxnType type) {
        dataset.begin(type);
    }

    public void begin(ReadWrite readWrite) {
        dataset.begin(readWrite);
    }

    public boolean promote(Promote mode) {
        return dataset.promote(mode);
    }

    public void commit() {
        dataset.commit();
    }

    public void abort() {
        dataset.abort();
    }

    public boolean isInTransaction() {
        return dataset.isInTransaction();
    }

    public void end() {
        dataset.end();
    }

    public ReadWrite transactionMode() {
        return dataset.transactionMode();
    }

    public TxnType transactionType() {
        return dataset.transactionType();
    }

    public DatasetGraph asDatasetGraph() {
        return dataset.asDatasetGraph();
    }

    public void close() {
        dataset.close();
    }

    public boolean isEmpty() {
        return dataset.isEmpty();
    }

    class NamedGraph {
        public Node name;
        public Graph g;

        public NamedGraph(Node name, Graph g) {
            this.name = name;
            this.g = g;
        }
    }
}
