package LLM.stream;

import org.streamreasoning.rsp4j.api.operators.s2r.execution.assigner.Consumer;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.util.ArrayList;
import java.util.List;

public class ImageStream implements DataStream<String> {

    List<Consumer<String>> consumerList = new ArrayList<>();
    String name;

    public ImageStream(String name){
        this.name = name;
    }

    @Override
    public void addConsumer(Consumer<String> consumer) {
        this.consumerList.add(consumer);
    }

    @Override
    public void put(String s, long l) {
        consumerList.forEach(c->c.notify(this, s, l));
    }

    @Override
    public String getName() {
        return name;
    }
}
