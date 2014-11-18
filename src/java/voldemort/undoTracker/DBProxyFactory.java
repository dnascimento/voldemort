package voldemort.undoTracker;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.Properties;

/**
 * Create and configure the DBProxy instance
 * 
 * @author darionascimento
 * 
 */
public class DBProxyFactory {

    static final String DB_PROXY_FILE = "dbProxy.obj";
    private static final String PROPERTIES_FILE = "undo.properties";

    public static DBProxy build() {
        Properties props = readPropertiesSaveGraph();

        DBProxy undoStub;
        if(props.getProperty("loadGraph").equals("true")) {
            undoStub = loadDBProxyFromFile(DB_PROXY_FILE);
        } else {
            undoStub = new DBProxy();
        }

        boolean showStatisticsAtEnd = props.getProperty("showGraphStatisticsAtExit").equals("true");
        boolean saveGraphAtEnd = props.getProperty("saveGraph").equals("true");
        if(showStatisticsAtEnd | saveGraphAtEnd) {
            Runtime.getRuntime().addShutdownHook(new ShuttleShutdownProcess(showStatisticsAtEnd,
                                                                            saveGraphAtEnd,
                                                                            undoStub,
                                                                            DB_PROXY_FILE));
        }
        return undoStub;
    }

    /**
     * Load the map from file
     * 
     * @param oUTPUT_FILE
     * 
     * @param loadFromFile
     * 
     * @return
     */
    public static DBProxy loadDBProxyFromFile(String file) {
        DBProxy proxy;
        try {
            FileInputStream fin = new FileInputStream(file);
            ObjectInputStream ois = new ObjectInputStream(fin);
            proxy = (DBProxy) ois.readObject();
            ois.close();
            System.out.println("DBProxy loaded from file: success");
            return proxy;
        } catch(Exception e) {
            System.out.println("DBProxy loaded from file: fail");
            return new DBProxy();
        }
    }

    public static Properties readPropertiesSaveGraph() {
        Properties props;
        InputStream is;
        try {
            is = new FileInputStream(PROPERTIES_FILE);
            props = new Properties();
            props.load(is);
            is.close();
            return props;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }
}
