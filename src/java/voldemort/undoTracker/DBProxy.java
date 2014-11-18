/*
 * Author: Dario Nascimento (dario.nascimento@tecnico.ulisboa.pt)
 * 
 * Instituto Superior Tecnico - University of Lisbon - INESC-ID Lisboa
 * Copyright (c) 2014 - All rights reserved
 */

package voldemort.undoTracker;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import objectexplorer.MemoryMeasurer;
import objectexplorer.ObjectGraphMeasurer;
import objectexplorer.ObjectGraphMeasurer.Footprint;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import voldemort.VoldemortException;
import voldemort.undoTracker.branching.BranchController;
import voldemort.undoTracker.branching.BranchPath;
import voldemort.undoTracker.branching.Path;
import voldemort.undoTracker.map.KeyMap;
import voldemort.undoTracker.map.KeyMapEntry;
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.VersionList;
import voldemort.undoTracker.map.VersionShuttle;
import voldemort.undoTracker.schedulers.NewScheduler;
import voldemort.undoTracker.schedulers.ReplayScheduler;
import voldemort.utils.ByteArray;

import com.google.protobuf.ByteString;

/**
 * Singleton class used by multiple threads
 * 
 * @author darionascimento
 * 
 */
public class DBProxy implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final InetSocketAddress MY_ADDRESS = getLocalAddress();
    public static final InetSocketAddress MANAGER_ADDRESS = new InetSocketAddress("manager", 11000);

    transient private static final Logger log = Logger.getLogger(DBProxy.class.getName());

    BranchController brancher = new BranchController();
    RestrainLocker restrainLocker = new RestrainLocker();

    ReplayScheduler replayScheduler;
    NewScheduler newScheduler;
    KeyMap keyMap;

    private boolean debugging;

    /**
     * One instance for multiple handlers
     * 
     * @param manager_address
     *        my_address
     *        s
     * 
     * @throws IOException
     */
    public DBProxy() {
        DOMConfigurator.configure("log4j.xml");
        LogManager.getRootLogger().setLevel(Level.WARN);
        log.setLevel(Level.DEBUG);
        debugging = false;

        log.info("New DBUndo stub at host" + MY_ADDRESS);
        this.keyMap = new KeyMap();

        replayScheduler = new ReplayScheduler(keyMap);
        newScheduler = new NewScheduler(this, keyMap);

        new SendDependencies(keyMap).start();
        try {
            new ServiceDBNode(this).start();
        } catch(IOException e) {
            log.error("DBUndoStub", e);
        }
    }

    public void startOperation(OpType op, ByteArray key, SRD srd) {
        if(srd.rid == 0) {
            if(debugging) {
                log.info(op + " " + hexStringToAscii(key) + " " + srd);
            }
            return;
        }
        StringBuilder sb = new StringBuilder();
        Path p = brancher.getPath(srd.branch);
        VersionShuttle version;
        if(p.isReplay) {
            // use replay branch
            if(debugging) {
                sb.append(" -> Replay: ");
                log.info("Replay Start: " + op + " " + hexStringToAscii(key) + " " + srd);
            }
            version = replayScheduler.startOperation(op, key.shadow(), srd, p.path);
        } else {
            if(debugging) {
                sb.append(" -> DO: ");
            }
            version = newScheduler.startOperation(op, key.shadow(), srd, p.path);
        }
        modifyKey(key, version.branch, version.sid);
        if(debugging) {
            sb.append(srd.rid);
            sb.append(" : ");
            sb.append(op);
            sb.append(" key: ");
            sb.append(hexStringToAscii(key));
            sb.append(" branch: ");
            sb.append(version.branch);
            sb.append(" snapshot: ");
            sb.append(version.sid);
            log.info(sb.toString());
        }
    }

    public void endOperation(OpType op, ByteArray key, SRD srd) {
        if(srd.rid != 0) {
            Path p = brancher.getPath(srd.branch);
            BranchPath path = p.path;
            removeKeyVersion(key);
            if(debugging) {
                log.info("Op end: " + op + " " + hexStringToAscii(key) + " " + srd);
            }
            if(p.isReplay) {
                replayScheduler.endOperation(op, key.shadow(), srd, path);
            } else {
                newScheduler.endOperation(op, key.shadow(), srd, path);
            }
        }
    }

    /**
     * Unlock operation to simulate the replay of these keys operations
     * 
     * @param keys
     * @param srd
     * @return
     * @throws VoldemortException
     */
    public Map<ByteArray, Boolean> unlockKeys(List<ByteArray> keys, SRD srd)
            throws VoldemortException {
        HashMap<ByteArray, Boolean> result = new HashMap<ByteArray, Boolean>();
        Path p = brancher.getPath(srd.branch);
        if(p.isReplay) {
            for(ByteArray key: keys) {
                log.info("unlock: " + ByteArray.toAscii(key) + "  - > " + srd);
                replayScheduler.ignore(key.shadow(), srd, p.path);
                result.put(key, true);
            }
        } else {
            log.error("Unlocking in the wrong branch: " + srd);
            for(ByteArray key: keys) {
                result.put(key, false);
            }
            throw new VoldemortException("Unlocking in the wrong branch: " + srd);
        }
        if(debugging) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unlocked: ");
            for(ByteArray key: keys) {
                sb.append(hexStringToAscii(key));
                sb.append(" : ");
            }
            sb.append("by request: ");
            sb.append(srd.rid);
            log.info(sb.toString());
        }
        return result;
    }

    /**
     * Blocks the thread until the restrain end.
     * Invoked by threads that process requests
     * 
     * @return
     */
    public BranchPath restrain(short branch) {
        restrainLocker.restrainRequest(branch);
        return brancher.getCurrent();
    }

    /**
     * Invoked by manager to start a new snapshot, in the current branch
     * 
     * @param newRid
     */
    public void scheduleNewSnapshot(long newRid) {
        String dateString = new SimpleDateFormat("H:m:S").format(new Date(newRid));
        log.info("Snapshot scheduled with rid: " + newRid + " at " + dateString);
        brancher.newSnapshot(newRid);
    }

    /**
     * Invoked by the manager before starting the replay
     * 
     * @param replayPath
     */
    public void newReplay(BranchPath replayPath) {
        log.info("New replay with path: " + replayPath);
        brancher.newReplay(replayPath);
    }

    /**
     * Replay is over
     * 
     * @param s
     * 
     * @param branch
     * @param sid
     */
    public void replayOver() {
        short newCurrentBranch = brancher.replayOver();
        // execute all pendent requests
        restrainLocker.replayOver(newCurrentBranch);
    }

    public static String hexStringToAscii(ByteArray key) {
        try {
            return new String(key.get(), "UTF-8");
        } catch(UnsupportedEncodingException e) {}
        return key.toString();
    }

    /**
     * Keyformat:
     * Xbytes - key
     * 2bytes - branch
     * 8bytes - sts
     * 
     * @param key
     * @param sid
     * @param branch
     */
    public static ByteArray modifyKey(ByteArray key, short branch, long snapshot) {
        byte[] oldKey = key.get();
        byte[] newKey = new byte[oldKey.length + 10];
        int i = 0;
        for(i = 0; i < oldKey.length; i++) {
            newKey[i] = oldKey[i];
        }

        newKey[i++] = (byte) (branch >> 8);
        newKey[i++] = (byte) (branch);

        newKey[i++] = (byte) (snapshot >> 56);
        newKey[i++] = (byte) (snapshot >> 48);
        newKey[i++] = (byte) (snapshot >> 40);
        newKey[i++] = (byte) (snapshot >> 32);
        newKey[i++] = (byte) (snapshot >> 24);
        newKey[i++] = (byte) (snapshot >> 16);
        newKey[i++] = (byte) (snapshot >> 8);
        newKey[i++] = (byte) (snapshot);
        key.set(newKey);
        return key;
    }

    public long getKeySnapshot(ByteArray key) {
        byte[] kb = key.get();
        if(kb.length < 8) {
            return -1;
        }
        byte[] longBytes = Arrays.copyOfRange(kb, kb.length - 8, kb.length);
        ByteBuffer bb = ByteBuffer.wrap(longBytes);
        return bb.getLong();
    }

    public static void removeKeyVersion(ByteArray key) {
        byte[] kb = key.get();

        byte[] longBytes = Arrays.copyOfRange(kb, 0, kb.length - 10);
        key.set(longBytes);
    }

    public void resetDependencies() {
        log.info("Reset dependency map");
        keyMap.clear();
        brancher.reset();
    }

    /**
     * Get the access list of a specific key for selective replay
     * 
     * @param keysList
     * @param baseRid
     * @return
     */
    public HashMap<ByteString, ArrayList<Op>> getAccessList(List<ByteString> keysList, long baseRid) {
        return keyMap.getOperationList(keysList, baseRid);
    }

    public void measureMemoryFootPrint() {
        Footprint footPrint = ObjectGraphMeasurer.measure(keyMap);
        long memory = MemoryMeasurer.measureBytes(keyMap);
        System.out.println("\n \n \n \n /************** MEMORY SUMMARY ******************\\");
        System.out.println("Total: \n" + "    " + footPrint);
        System.out.println("     memory" + memory + " bytes");
        System.out.println("------");
        Enumeration<ByteArray> list = keyMap.getKeySet();
        long opListSize = 0;
        long snapshotListSize = 0;
        long counter = 0;
        long opListEntries = 0;
        long snapshotListEntries = 0;

        while(list.hasMoreElements()) {
            counter++;
            ByteArray key = list.nextElement();
            KeyMapEntry entry = keyMap.get(key);

            // System.out.println(ObjectGraphMeasurer.measure(entry.getOperationList()));
            // System.out.println(ObjectGraphMeasurer.measure(entry.getSnapshotList()));
            ArrayList<Op> opList = entry.operationList;
            opListSize += MemoryMeasurer.measureBytes(opList);
            opListEntries += opList.size();

            VersionList snapshotList = entry.versionList;
            snapshotListSize += MemoryMeasurer.measureBytes(snapshotList);
            snapshotListEntries += snapshotList.size();
        }

        System.out.println("Number of database keys: " + counter);
        System.out.println("Total opList: " + opListSize + " bytes");
        System.out.println("Total snapshotList: " + snapshotListSize + " bytes");
        System.out.println("Entries in opList: " + opListEntries);
        System.out.println("Entries in snapshotList: " + snapshotListEntries);
        System.out.println("/********************************\\ \n \n \n \n ");

        log.info("Saving key access list....");
    }

    private static InetSocketAddress getLocalAddress() {
        try {
            String address = getAddress();
            System.out.println("Staring service on address:" + address);
            return new InetSocketAddress(address, 11200);
        } catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println("Staring service on address: localhost");
        return new InetSocketAddress("localhost", 11200);
    }

    public static String getAddress() throws Exception {
        // 3 cases: only 127.0 , only a non 127 or various
        List<String> validIp = new ArrayList<String>();

        Pattern pattern = Pattern.compile("\\d*\\.\\d*\\.\\d*\\.\\d*");

        Enumeration<NetworkInterface> n = NetworkInterface.getNetworkInterfaces();
        for(; n.hasMoreElements();) {
            NetworkInterface e = n.nextElement();

            Enumeration<InetAddress> a = e.getInetAddresses();
            for(; a.hasMoreElements();) {
                String addr = a.nextElement().getHostAddress();
                Matcher m = pattern.matcher(addr);
                if(m.matches()) {
                    validIp.add(addr);
                }

            }
        }
        switch(validIp.size()) {
            case 0:
                throw new Exception("No ip v4 founded");
            case 1:
                return validIp.get(0);
            case 2:
                validIp.remove("127.0.0.1");
                return validIp.get(0);

        }
        // various interfaces/ips
        validIp.remove("127.0.0.1");

        String localIp = null;
        boolean oneLocalIp = true;
        for(String ip: validIp) {
            if(ip.startsWith("192.168.")) {
                if(localIp != null) {
                    oneLocalIp = false;
                }
                localIp = ip;
            }
        }

        if(localIp != null && oneLocalIp) {
            return localIp;
        }
        // choose one ip
        System.out.println("Select interface: ");
        for(int i = 0; i < validIp.size(); i++) {
            System.out.println(i + ")" + validIp.get(i));
        }
        Scanner s = new Scanner(System.in);
        int option = s.nextInt();
        s.close();
        return validIp.get(option);
    }

}
