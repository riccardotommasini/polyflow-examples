package LLM.customoperators;

import LLM.customdatatypes.ImageDescription_scenegraph;
import dev.langchain4j.data.message.*;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.model.output.Response;
import org.json.JSONArray;
import org.streamreasoning.rsp4j.api.operators.r2r.RelationToRelationOperator;

import java.util.ArrayList;
import java.util.List;

import static dev.langchain4j.model.openai.OpenAiChatModelName.GPT_4_O_MINI;

public class LLMOperator_scenegraph implements RelationToRelationOperator<ImageDescription_scenegraph> {

    List<String> tvgNames;
    String resName;
    String query;
    ChatLanguageModel model;
    JSONArray sceneGraphs;

    public LLMOperator_scenegraph(String query, List<String> tvgNames, String resName){
        this.query = query;
        this.tvgNames = tvgNames;
        this.resName = resName;
        this.model = OpenAiChatModel.builder()
                .apiKey(System.getenv("OPENAI_KEY"))
                .modelName(GPT_4_O_MINI)
                .maxTokens(500)
                .temperature(0.4)
                .build();
    }

    @Override
    public ImageDescription_scenegraph eval(List<ImageDescription_scenegraph> list) {
        List<Content> msgContent = new ArrayList<>();
        ImageDescription_scenegraph result = new ImageDescription_scenegraph();
        msgContent.add(TextContent.from(query));
        if(!list.get(0).getImages().isEmpty()) {
            for (String encodedImage : list.get(0).getImages()) {
                msgContent.add(ImageContent.from(encodedImage, "image/png"));
            }
            UserMessage msg = UserMessage.from(msgContent);
            Response<AiMessage> response = model.generate(msg);
            System.out.println(response.content().text());
            this.sceneGraphs = new JSONArray(response.content().text());

            for (Object s : sceneGraphs) {
                result.addSceneGraph(s.toString());
            }
        }
        return result;
    }

    @Override
    public List<String> getTvgNames() {
        return tvgNames;
    }

    @Override
    public String getResName() {
        return resName;
    }
}
