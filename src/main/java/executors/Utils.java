package executors;

import exceptions.IncorrectBTreePageType;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Utils {
  public static String getLastTableNameFromFile(FileInputStream fileInputStream)
      throws IOException {
    while (fileInputStream.available() > 0) {

    }
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

  public static String[] getTableNamesFromRecords(byte[] records, int length) {
    String[] tableNames = new String[length];
    ByteBuffer recordBuffer = ByteBuffer.wrap(records);

    int i = 0;
    int offset = 0;
    while (recordBuffer.hasRemaining() && i < length) {
      byte payloadSize = recordBuffer.get();
      // Skip rowid
      recordBuffer.get();
      // Payload starts here
      byte payloadHeaderSize = recordBuffer.get();
      byte typeSize = getSize(recordBuffer.get());
      byte nameSize = getSize(recordBuffer.get());
      byte tableNameSize = getSize(recordBuffer.get());
      // Skip rest record header
      recordBuffer.get(new byte[payloadHeaderSize - 4]);
      // Record body starts here
      // Skip sqlite_schema.type from body
      byte[] typeBytes = new byte[typeSize];
      recordBuffer.get(typeBytes);
      // Skip sqlite_schema.name
      byte[] nameBytes = new byte[nameSize];
      recordBuffer.get(nameBytes);
      // Get table name sqlite_schema.tbl_name
      byte[] tableNameBytes = new byte[tableNameSize];
      recordBuffer.get(tableNameBytes);
      tableNames[i++] = new String(tableNameBytes);
      // Skip the rest of the record body
      int skipSize = payloadSize + 2 - (recordBuffer.position() - offset);
      recordBuffer.get(new byte[skipSize]);
      offset = recordBuffer.position();
    }
    return tableNames;
  }

  private static int getSize(int serialType) {
    if (isOddAndBiggerThan13(serialType)) {
      return (byte) ((serialType - 13) / 2);
    } else {
      return (byte) ((serialType - 12) / 2);
    }
  }

  private static byte getSize(byte serialType) {
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
