package struct.cells;

import java.util.List;

public interface IndexCell extends Cell {

  RecordHeader getRecordHeader();

  RecordBody getRecordBody();

  record RecordHeader(int payloadHeaderSize, List<Integer> serialTypes) {

  }

  record RecordBody(byte[][] values) {

  }

}
