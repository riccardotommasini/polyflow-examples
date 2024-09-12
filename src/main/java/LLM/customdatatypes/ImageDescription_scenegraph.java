package LLM.customdatatypes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ImageDescription_scenegraph implements Iterable<String>{

    private List<String> images = new ArrayList<>();
    private List<String> sceneGraphs = new ArrayList<>();

    public List<String> getImages(){
        return this.images;
    }
    public List<String> getSceneGraphs(){
        return this.sceneGraphs;
    }
    public void addSceneGraph(String sceneGraph){
        this.sceneGraphs.add(sceneGraph);
    }
    public void addUrl(String url){
        this.images.add(url);
    }
    public void addAll(ImageDescription_scenegraph img){
        this.images.addAll(img.getImages());
    }


    @Override
    public Iterator<String> iterator() {
        return sceneGraphs.iterator();
    }
}
