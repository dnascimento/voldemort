package voldemort;

import java.util.Scanner;

import javax.xml.bind.DatatypeConverter;

import voldemort.undoTracker.DBUndoStub;
import voldemort.utils.ByteArray;

public class KeyConvert {

    public static void main(String[] args) {
        System.out.println("Enter key:");
        Scanner s = new Scanner(System.in);
        String sl = s.nextLine();
        DatatypeConverter.parseHexBinary(sl);
        ByteArray key = new ByteArray(DatatypeConverter.parseHexBinary(sl));
        System.out.println(DBUndoStub.hexStringToAscii(key));
        s.close();
    }
}
