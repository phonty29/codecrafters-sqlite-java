package executors;

import exceptions.IncorrectBTreePageType;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Utils {
  public static String getLastTableNameFromFile(FileInputStream fileInputStream)
      throws IOException {
    int payloadSize = fileInputStream.read();
    // Skip rowid
    fileInputStream.read();
    // Payload starts here
    int payloadHeaderSize = fileInputStream.read();
    int typeSize = getSize(fileInputStream.read());
    int nameSize = getSize(fileInputStream.read());
    int tableNameSize = getSize(fileInputStream.read());
    // Skip rest record header
    fileInputStream.read(new byte[payloadHeaderSize - 4]);
    // Record body starts here
    // Skip sqlite_schema.type from body
    byte[] typeBytes = new byte[typeSize];
    fileInputStream.read(typeBytes);
    // Skip sqlite_schema.name
    byte[] nameBytes = new byte[nameSize];
    fileInputStream.read(nameBytes);
    // Get table name sqlite_schema.tbl_name
    byte[] tableNameBytes = new byte[tableNameSize];
    fileInputStream.read(tableNameBytes);
    return new String(tableNameBytes);
  }


  public static int getSize(int serialType) {
    if (isOddAndBiggerThan13(serialType)) {
      return (byte) ((serialType - 13) / 2);
    } else {
      return (byte) ((serialType - 12) / 2);
    }
  }

  public static byte getSize(byte serialType) {
    if (isOddAndBiggerThan13(serialType)) {
      return (byte) ((serialType - 13) / 2);
    } else {
      return (byte) ((serialType - 12) / 2);
    }
  }

  private static boolean isOddAndBiggerThan13(byte b) {
    return b > 13 && (b%2 == 0);
  }

  private static boolean isOddAndBiggerThan13(int b) {
    return b > 13 && (b%2 == 0);
  }

  public static void validateBTreePageType(byte type) throws IncorrectBTreePageType {
    if (type != 0x02 && type != 0x05 && type != 0x0a && type != 0x0d) {
      throw new IncorrectBTreePageType(type);
    }
  }
}
