package voldemort.undoTracker;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * This class is a Shutdown hook, which is invoked when the database node is
 * terminated. It backups the metadata store and/or shows the statistics
 * 
 * @author darionascimento
 * 
 */
public class ShuttleShutdownProcess extends Thread {

    private DBProxy undostub;
    private String dbProxyFile;
    private boolean showStatisticsAtEnd;
    private boolean saveGraphAtEnd;

    public ShuttleShutdownProcess(boolean showStatisticsAtEnd,
                                  boolean saveGraphAtEnd,
                                  DBProxy undostub,
                                  String dbProxyFile) {
        this.undostub = undostub;
        this.showStatisticsAtEnd = showStatisticsAtEnd;
        this.saveGraphAtEnd = saveGraphAtEnd;
        this.dbProxyFile = dbProxyFile;
    }

    @Override
    public void run() {
        save();
    }

    public void save() {
        if(showStatisticsAtEnd) {
            undostub.measureMemoryFootPrint();
        }
        if(saveGraphAtEnd) {
            try {
                FileOutputStream fout = new FileOutputStream(dbProxyFile);
                ObjectOutputStream oos = new ObjectOutputStream(fout);
                oos.writeObject(undostub);
                oos.close();
            } catch(FileNotFoundException e) {
                e.printStackTrace();
            } catch(IOException e) {
                e.printStackTrace();
            }
        }
    }
}
