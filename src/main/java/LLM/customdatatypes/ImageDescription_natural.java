package LLM.customdatatypes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ImageDescription_natural implements Iterable<String>{
    private List<String> images = new ArrayList<>();
    private List<String> answer = new ArrayList<>();

    public List<String> getImages(){
        return this.images;
    }
    public List<String> getAnswer(){
        return this.answer;
    }
    public void addAnswer(String answer){
        this.answer.add(answer);
    }
    public void addUrl(String url){
        this.images.add(url);
    }
    public void addAll(ImageDescription_natural img){
        this.images.addAll(img.getImages());
    }


    @Override
    public Iterator<String> iterator() {
        return answer.iterator();
    }
}
