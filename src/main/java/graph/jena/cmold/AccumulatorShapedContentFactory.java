package graph.jena.cmold;

import graph.jena.datatypes.JenaGraphOrBindings;
import org.apache.jena.graph.Graph;
import org.apache.jena.shacl.ShaclValidator;
import org.streamreasoning.polyflow.api.operators.s2r.execution.state.Segment;
import org.streamreasoning.polyflow.api.operators.s2r.execution.state.SegmentFactory;
import org.streamreasoning.polyflow.base.operatorsimpl.s2r.segment.EmptySegment;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class AccumulatorShapedContentFactory implements SegmentFactory<Graph, JenaGraphOrBindings> {

    private final Function<Graph, Graph> inputMapper;
    private final Function<Graph, JenaGraphOrBindings> resultMapper;
    private final BiFunction<JenaGraphOrBindings, JenaGraphOrBindings, JenaGraphOrBindings> merger;
    private final JenaGraphOrBindings emptyContent;
    private final Graph shapesGraph;

    public AccumulatorShapedContentFactory(Graph shapesGraph, Function<Graph, Graph> inputMapper, Function<Graph, JenaGraphOrBindings> resultMapper,
                                           BiFunction<JenaGraphOrBindings, JenaGraphOrBindings, JenaGraphOrBindings> merger, JenaGraphOrBindings emptyContent) {
        this.shapesGraph = shapesGraph;
        this.inputMapper = inputMapper;
        this.resultMapper = resultMapper;
        this.merger = merger;
        this.emptyContent = emptyContent;
    }

    @Override
    public Segment<Graph, JenaGraphOrBindings> createEmpty() {
        return new EmptySegment<>(emptyContent);
    }

    @Override
    public Segment<Graph, JenaGraphOrBindings> create() {
        return new AccumulatorShapedSegment(inputMapper, resultMapper, merger, emptyContent);
    }

    class AccumulatorShapedSegment implements Segment<Graph, JenaGraphOrBindings> {
        private final Function<Graph, Graph> inputMapper;
        private final Function<Graph, JenaGraphOrBindings> resultMapper;
        private final BiFunction<JenaGraphOrBindings, JenaGraphOrBindings, JenaGraphOrBindings> merger;
        private final JenaGraphOrBindings emptyContent;
        private final List<Graph> content = new ArrayList<>();

        AccumulatorShapedSegment(Function<Graph, Graph> inputMapper, Function<Graph, JenaGraphOrBindings> resultMapper,
                                 BiFunction<JenaGraphOrBindings, JenaGraphOrBindings, JenaGraphOrBindings> merger, JenaGraphOrBindings emptyContent) {
            this.inputMapper = inputMapper;
            this.resultMapper = resultMapper;
            this.merger = merger;
            this.emptyContent = emptyContent;
        }

        @Override
        public int size() {
            return content.size();
        }

        @Override
        public void add(Graph item) {
            content.add(inputMapper.apply(item));
        }

        @Override
        public JenaGraphOrBindings coalesce() {
            JenaGraphOrBindings result = emptyContent;
            for (Graph item : content) {
                result = merger.apply(result, resultMapper.apply(item));
            }

            //TODO here we can optimise and fail sooner than the final graph
            if (ShaclValidator.get().conforms(shapesGraph, result.getContent()))
                return result;
            else
                return emptyContent;
        }

        @Override
        public Iterator<Graph> iterator() {
            return Segment.super.iterator();
        }
    }
}
