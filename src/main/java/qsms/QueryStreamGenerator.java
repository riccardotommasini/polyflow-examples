package qsms;

import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public class QueryStreamGenerator {
    static String dir = JenaQSMS.class.getResource("/bgp").getPath();

    private static final String PREFIX = "http://test/";
    private static final Long TIMEOUT = 1000L;
    private final String[] colors = new String[]{"Blue", "Green", "Red", "Yellow", "Black", "Grey", "White"};
    private final Map<String, DataStream<String>> activeStreams = new HashMap();
    private final AtomicBoolean isStreaming = new AtomicBoolean(false);
    private final Random randomGenerator = new Random(1336L);
    private AtomicLong streamIndexCounter = new AtomicLong(0L);
    private String prefixes;
    private String[] fileNames = new String[]{"/activity.trig", "/location.trig", "/heart.trig", "/breathing.trig", "/oxygen.trig"};
    private List<Scanner> scanners = new ArrayList();

    public QueryStreamGenerator() {
        try {
            for (int i = 0; i < this.fileNames.length; ++i) {
                this.scanners.add(new Scanner(new File(graph.jena.stream.JenaStreamGenerator.class.getResource(this.fileNames[i]).getPath())));
                this.prefixes = ((Scanner) this.scanners.get(i)).nextLine();
            }
        } catch (FileNotFoundException var2) {
        }

    }

    public static String getPREFIX() {
        return "http://test/";
    }

    public DataStream<String> getStream(String streamURI) {
        if (!this.activeStreams.containsKey(streamURI)) {
            DataStream<String> stream = new StringStream(streamURI);
            this.activeStreams.put(streamURI, stream);
        }

        return (DataStream) this.activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0L;
                try {
                    while (this.isStreaming.get()) {
                        for (DataStream<String> stream : this.activeStreams.values()) {
                            this.generateDataAndAddToStream(stream, ts);
                            ts += 200L;
                        }
                        Thread.sleep(TIMEOUT);
                    }
                } catch (InterruptedException var6) {
                    InterruptedException e = var6;
                    e.printStackTrace();
                }
            };
            Thread thread = new Thread(task);
            thread.start();
        }

    }

    private void generateDataAndAddToStream(DataStream<String> stream, long ts) {
        try {
            List<String> list = Stream.of(Objects.requireNonNull(new File(dir).listFiles()))
                    .filter(file -> !file.isDirectory())
                    .map(File::getName)
                    .toList();
            String e = list.get(randomGenerator.nextInt(list.size() - 1));
            String queryString = new String(Files.readAllBytes(Paths.get(dir + "/" + e)));
            stream.put(queryString, ts);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }


//        RDF instance = RDFUtils.getInstance();
//        Graph graph = GraphMemFactory.createGraphMem();
//        Node p = NodeFactory.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
//        if (stream.getName().equals("http://test/stream1")) {
//            graph.add(NodeFactory.createURI("http://test/S" + this.streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI("http://test/" + this.selectRandomColor()));
//            graph.add(NodeFactory.createURI("http://test/S" + this.streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI("http://test/Black"));
//            stream.put(graph, ts);
//        } else if (stream.getName().equals("http://test/stream2")) {
//            graph.add(NodeFactory.createURI("http://test/S" + this.streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI("http://test/" + this.randomGenerator.nextInt(10)));
//            graph.add(NodeFactory.createURI("http://test/S" + this.streamIndexCounter.incrementAndGet()), p, NodeFactory.createURI("http://test/0"));
//            stream.put(graph, ts);
//        } else if (stream.getName().equals("http://test/RDFstar")) {
//            Iterator var7 = this.scanners.iterator();
//
//            while (var7.hasNext()) {
//                Scanner s = (Scanner) var7.next();
//                String data = s.nextLine();
//                Graph tmp = GraphMemFactory.createGraphMem();
//                DatasetGraph ds = new DatasetGraphInMemory();
//                RDFParser.create().base("http://base/").source(new ByteArrayInputStream((this.prefixes + data).getBytes())).checking(false).lang(RDFLanguages.TRIG).parse(ds);
//                ds.stream().forEach((g) -> {
//                    tmp.add(g.asTriple());
//                });
//                stream.put(tmp, ts);
//            }
//        }

    }

    private String selectRandomColor() {
        int randomIndex = this.randomGenerator.nextInt(this.colors.length);
        return this.colors[randomIndex];
    }

    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}
