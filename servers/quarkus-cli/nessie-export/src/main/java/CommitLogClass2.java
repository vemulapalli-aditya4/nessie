import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;

import java.io.Serializable;
import java.util.List;

public class CommitLogClass2 implements Serializable {

    public List<Content> contents;

    public CommitLogClass2() {
    }

    public CommitMeta commitMeta;

    public CommitLogClass2(List<Content> contents, CommitMeta commitMeta) {
        this.contents = contents;
        this.commitMeta = commitMeta;
    }
}
