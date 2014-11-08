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
import java.util.concurrent.locks.ReentrantLock;
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
import voldemort.undoTracker.map.Op;
import voldemort.undoTracker.map.Op.OpType;
import voldemort.undoTracker.map.OpMultimap;
import voldemort.undoTracker.map.OpMultimapEntry;
import voldemort.undoTracker.map.StsBranchPair;
import voldemort.undoTracker.map.commits.CommitList;
import voldemort.undoTracker.schedulers.CommitScheduler;
import voldemort.undoTracker.schedulers.RedoScheduler;
import voldemort.undoTracker.schedulers.RestrainScheduler;
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

    ReentrantLock restrainLocker = new ReentrantLock();
    BranchController brancher = new BranchController();

    RedoScheduler redoScheduler;
    CommitScheduler newRequestsScheduler;
    OpMultimap keyAccessLists;
    RestrainScheduler restrainScheduler;

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
        this.keyAccessLists = new OpMultimap();

        redoScheduler = new RedoScheduler(keyAccessLists);
        newRequestsScheduler = new CommitScheduler(keyAccessLists);
        restrainScheduler = new RestrainScheduler(keyAccessLists, restrainLocker);

        new SendDependencies(keyAccessLists).start();
        try {
            new ServiceDBNode(this).start();
        } catch(IOException e) {
            log.error("DBUndoStub", e);
        }
    }

    private void opStart(OpType op, ByteArray key, SRD srd) {
        if(srd.rid == 0) {
            if(debugging) {
                log.info(op + " " + hexStringToAscii(key) + " with srd: " + srd);
            }
            return;
        }

        StringBuilder sb = new StringBuilder();
        // System.out.println("-> " + op + " " + ByteArray.toAscii(key) + " - "
        // + srd);
        Path p = brancher.getPath(srd.branch);
        BranchPath path = p.path;
        StsBranchPair access;
        if(p.isRedo) {
            // use redo branch
            if(debugging) {
                sb.append(" -> REDO: ");
                log.info("Redo Start: " + op + " " + hexStringToAscii(key) + " " + srd);
            }
            access = redoScheduler.opStart(op, key.shadow(), srd, path);
        } else {
            if(srd.restrain) {
                if(debugging) {
                    System.out.println(" -> RESTRAIN: ");
                }
                // new request but may need to wait to avoid dirty reads
                restrainScheduler.opStart(op, key.shadow(), srd, path);
                path = brancher.getCurrent();
                srd.branch = path.current.branch; // update the branch
            }
            if(debugging) {
                sb.append(" -> DO: ");
            }
            // Read old/common branch, may create a new commit - for new
            // requests
            access = newRequestsScheduler.opStart(op, key.shadow(), srd, path);
        }
        modifyKey(key, access.branch, access.sts);
        if(debugging) {
            sb.append(srd.rid);
            sb.append(" : ");
            sb.append(op);
            sb.append(" key: ");
            sb.append(hexStringToAscii(key));
            sb.append(" branch: ");
            sb.append(access.branch);
            sb.append(" commit: ");
            sb.append(access.sts);
            log.info(sb.toString());
        }
    }

    public void opEnd(OpType op, ByteArray key, SRD srd) {
        if(srd.rid != 0) {
            Path p = brancher.getPath(srd.branch);
            BranchPath path = p.path;
            removeKeyVersion(key);
            if(debugging) {
                log.info("Op end: " + op + " " + hexStringToAscii(key) + " " + srd);
            }
            if(p.isRedo) {
                redoScheduler.opEnd(op, key.shadow(), srd, path);
            } else {
                if(srd.restrain) {
                    restrainScheduler.opEnd(op, key.shadow(), srd, path);
                }
                newRequestsScheduler.opEnd(op, key.shadow(), srd, path);
            }
        }
    }

    /**
     * Unlock operation to simulate the redo of these keys operations
     * 
     * @param keys
     * @param srd
     * @return
     * @throws VoldemortException
     */
    public Map<ByteArray, Boolean> unlockKey(List<ByteArray> keys, SRD srd)
            throws VoldemortException {
        HashMap<ByteArray, Boolean> result = new HashMap<ByteArray, Boolean>();
        Path p = brancher.getPath(srd.branch);
        if(p.isRedo) {
            for(ByteArray key: keys) {
                log.info("unlock: " + ByteArray.toAscii(key) + "  - > " + srd);
                boolean status = redoScheduler.ignore(key.shadow(), srd, p.path);
                result.put(key, status);
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
     * Invoked by manager to start a new commit, in the current branch
     * 
     * @param newRid
     */
    public void scheduleNewCommit(long newRid) {
        String dateString = new SimpleDateFormat("H:m:S").format(new Date(newRid));
        log.info("Commit scheduled with rid: " + newRid + " at " + dateString);
        brancher.newCommit(newRid);
    }

    /**
     * Invoked by the manager before starting the replay
     * 
     * @param redoPath
     */
    public void newRedo(BranchPath redoPath) {
        log.info("New redo with path: " + redoPath);
        brancher.newRedo(redoPath);
    }

    /**
     * Redo is over
     * 
     * @param s
     * 
     * @param branch
     * @param sts
     */
    public void redoOver() {
        brancher.redoOver();
        // execute all pendent requests
        synchronized(restrainLocker) {
            restrainLocker.notifyAll();
        }
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
     * @param sts
     * @param branch
     */
    public static ByteArray modifyKey(ByteArray key, short branch, long commit) {
        byte[] oldKey = key.get();
        byte[] newKey = new byte[oldKey.length + 10];
        int i = 0;
        for(i = 0; i < oldKey.length; i++) {
            newKey[i] = oldKey[i];
        }

        newKey[i++] = (byte) (branch >> 8);
        newKey[i++] = (byte) (branch);

        newKey[i++] = (byte) (commit >> 56);
        newKey[i++] = (byte) (commit >> 48);
        newKey[i++] = (byte) (commit >> 40);
        newKey[i++] = (byte) (commit >> 32);
        newKey[i++] = (byte) (commit >> 24);
        newKey[i++] = (byte) (commit >> 16);
        newKey[i++] = (byte) (commit >> 8);
        newKey[i++] = (byte) (commit);
        key.set(newKey);
        return key;
    }

    public long getKeyCommit(ByteArray key) {
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
        keyAccessLists.clear();
        brancher.reset();
    }

    public void getStart(ByteArray key, SRD srd) {
        opStart(OpType.Get, key, srd);
    }

    public void getEnd(ByteArray key, SRD srd) {
        opEnd(OpType.Get, key, srd);
    }

    public void putStart(ByteArray key, SRD srd) {
        opStart(OpType.Put, key, srd);
    }

    public void putEnd(ByteArray key, SRD srd) {
        opEnd(OpType.Put, key, srd);
    }

    public void deleteStart(ByteArray key, SRD srd) {
        opStart(OpType.Delete, key, srd);
    }

    public void deleteEnd(ByteArray key, SRD srd) {
        opEnd(OpType.Delete, key, srd);
    }

    public void getVersion(ByteArray key, SRD srd) {
        opStart(OpType.GetVersion, key, srd);
    }

    public void getVersionEnd(ByteArray key, SRD srd) {
        opEnd(OpType.GetVersion, key, srd);
    }

    /**
     * Get the access list of a specific key for selective replay
     * 
     * @param keysList
     * @param baseRid
     * @return
     */
    public HashMap<ByteString, ArrayList<Op>> getAccessList(List<ByteString> keysList, long baseRid) {
        return keyAccessLists.getAccessList(keysList, baseRid);
    }

    public void measureMemoryFootPrint() {
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
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brancher == null) ? 0 : brancher.hashCode());
        result = prime * result + (debugging ? 1231 : 1237);
        result = prime * result + ((keyAccessLists == null) ? 0 : keyAccessLists.hashCode());
        result = prime * result
                 + ((newRequestsScheduler == null) ? 0 : newRequestsScheduler.hashCode());
        result = prime * result + ((redoScheduler == null) ? 0 : redoScheduler.hashCode());
        result = prime * result + ((restrainLocker == null) ? 0 : restrainLocker.hashCode());
        result = prime * result + ((restrainScheduler == null) ? 0 : restrainScheduler.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if(this == obj)
            return true;
        if(obj == null)
            return false;
        if(getClass() != obj.getClass())
            return false;
        DBProxy other = (DBProxy) obj;
        if(brancher == null) {
            if(other.brancher != null)
                return false;
        } else if(!brancher.equals(other.brancher))
            return false;
        if(debugging != other.debugging)
            return false;
        if(keyAccessLists == null) {
            if(other.keyAccessLists != null)
                return false;
        } else if(!keyAccessLists.equals(other.keyAccessLists))
            return false;
        if(newRequestsScheduler == null) {
            if(other.newRequestsScheduler != null)
                return false;
        } else if(!newRequestsScheduler.equals(other.newRequestsScheduler))
            return false;
        if(redoScheduler == null) {
            if(other.redoScheduler != null)
                return false;
        } else if(!redoScheduler.equals(other.redoScheduler))
            return false;
        if(restrainScheduler == null) {
            if(other.restrainScheduler != null)
                return false;
        } else if(!restrainScheduler.equals(other.restrainScheduler))
            return false;
        return true;
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
