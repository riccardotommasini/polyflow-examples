package LLM.stream;

import org.streamreasoning.rsp4j.api.stream.data.DataStream;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImageStreamGenerator {

    private static final Long TIMEOUT = 1000l;
    private final Map<String, DataStream<String>> activeStreams;
    private final AtomicBoolean isStreaming;
    private List<String> images = new ArrayList<>();
    int count = 0;
    int num_images = 8;

    public ImageStreamGenerator() {
        this.activeStreams = new HashMap<>();
        this.isStreaming = new AtomicBoolean(false);
        for(int i = 1; i<=num_images; i++) {
            try {
                BufferedImage image = ImageIO.read(new File(ImageStreamGenerator.class.getResource("/img" + i + ".png").getPath()));
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ImageIO.write(image, "png", baos);
                byte[] imageBytes = baos.toByteArray();
                images.add(Base64.getEncoder().encodeToString(imageBytes));
            }catch(Exception e){
                e.printStackTrace();
            }
        }

    }



    public DataStream<String> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            ImageStream stream = new ImageStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming.get()) {
                    if(count >=images.size())
                        count = 0;
                    long finalTs = ts;
                    activeStreams.entrySet().forEach(e -> generateDataAndAddToStream(e.getValue(), finalTs));
                    ts += 300;
                    count+=1;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                stopStreaming();

            };


            Thread thread = new Thread(task);
            thread.start();
        }
    }

    private void generateDataAndAddToStream(DataStream<String> stream, long ts) {
        stream.put(images.get(count), ts);
    }


    public void stopStreaming() {
        this.isStreaming.set(false);
    }
}
