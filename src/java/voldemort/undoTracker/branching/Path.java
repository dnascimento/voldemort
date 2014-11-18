package voldemort.undoTracker.branching;

public class Path {

    public BranchPath path;
    public boolean isReplay;

    public Path(BranchPath path, boolean replay) {
        super();
        this.path = path;
        this.isReplay = replay;
    }

}
