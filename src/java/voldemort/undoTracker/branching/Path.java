package voldemort.undoTracker.branching;

public class Path {

    public BranchPath path;
    public boolean isRedo;

    public Path(BranchPath path, boolean redo) {
        super();
        this.path = path;
        this.isRedo = redo;
    }

}
