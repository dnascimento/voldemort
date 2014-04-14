package voldemort.undoTracker;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import voldemort.undoTracker.map.OpMultimap;

public class SaveKeyAccess extends Thread {

    private OpMultimap keyAccessLists;

    public SaveKeyAccess(OpMultimap keyAccessLists) {
        this.keyAccessLists = keyAccessLists;
    }

    @Override
    public void run() {
        try {
            System.out.println("Saving key access list....");
            FileOutputStream fout = new FileOutputStream(DBUndoStub.KEY_ACCESS_LIST_FILE);
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(keyAccessLists);
            oos.close();
        } catch(FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch(IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
