package voldemort.undoTracker;

import java.util.ArrayList;
import java.util.Enumeration;

import objectexplorer.MemoryMeasurer;
import objectexplorer.ObjectGraphMeasurer;
import objectexplorer.ObjectGraphMeasurer.Footprint;

import org.apache.log4j.Logger;

import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapEntry;
import voldemort.undoTracker.map.commits.CommitList;
import voldemort.utils.ByteArray;

public class SaveKeyAccess extends Thread {

    private static final Logger log = Logger.getLogger(SaveKeyAccess.class.getName());

    private OpMultimap keyAccessLists;

    public SaveKeyAccess(OpMultimap keyAccessLists) {
        this.keyAccessLists = keyAccessLists;
    }

    @Override
    public void run() {
        // try {
        // calculate the size
        Footprint footPrint = ObjectGraphMeasurer.measure(keyAccessLists);
        long memory = MemoryMeasurer.measureBytes(keyAccessLists);
        System.out.println("\n \n \n \n /************** MEMORY SUMMARY ******************\\");
        System.out.println("Total: \n" + "    " + footPrint);
        System.out.println("     memory" + memory + " bytes");
        System.out.println("------");
        Enumeration<ByteArray> list = keyAccessLists.getKeySet();
        long opListSize = 0;
        long commitListSize = 0;
        long counter = 0;
        long opListEntries = 0;
        long commitListEntries = 0;

        while(list.hasMoreElements()) {
            counter++;
            ByteArray key = list.nextElement();
            OpMultimapEntry entry = keyAccessLists.get(key);

            // System.out.println(ObjectGraphMeasurer.measure(entry.getOperationList()));
            // System.out.println(ObjectGraphMeasurer.measure(entry.getCommitList()));
            ArrayList<Op> opList = entry.getOperationList();
            opListSize += MemoryMeasurer.measureBytes(opList);
            opListEntries += opList.size();

            CommitList commitList = entry.getCommitList();
            commitListSize += MemoryMeasurer.measureBytes(commitList);
            commitListEntries += commitList.size();
        }

        System.out.println("Number of database keys: " + counter);
        System.out.println("Total opList: " + opListSize + " bytes");
        System.out.println("Total commitList: " + commitListSize + " bytes");
        System.out.println("Entries in opList: " + opListEntries);
        System.out.println("Entries in commitList: " + commitListEntries);
        System.out.println("/********************************\\ \n \n \n \n ");

        log.info("Saving key access list....");

        /*
         * ------ Uncomment to store the access lists in Disk
         * FileOutputStream fout = new
         * FileOutputStream(DBProxy.KEY_ACCESS_LIST_FILE);
         * ObjectOutputStream oos = new ObjectOutputStream(fout);
         * oos.writeObject(keyAccessLists);
         * oos.close();
         * } catch(FileNotFoundException e) {
         * // TODO Auto-generated catch block
         * e.printStackTrace();
         * } catch(IOException e) {
         * // TODO Auto-generated catch block
         * e.printStackTrace();
         * }
         */

    }
}
