package qsms;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class StringStream implements DataStream<String> {
    protected String stream_uri;
    protected List<Consumer<String>> consumers = new ArrayList();

    public StringStream(String stream_uri) {
        this.stream_uri = stream_uri;
    }

    public void addConsumer(Consumer<String> c) {
        this.consumers.add(c);
    }

    public void put(String e, long ts) {
        this.consumers.forEach((graphConsumer) -> graphConsumer.notify(this, e, ts));
    }

    public String getName() {
        return this.stream_uri;
    }

    public String uri() {
        return this.stream_uri;
    }
}