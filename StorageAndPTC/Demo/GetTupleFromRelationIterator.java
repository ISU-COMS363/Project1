package Demo;

import PTCFramework.ProducerIterator;
import StorageManager.Storage;

public class GetTupleFromRelationIterator implements ProducerIterator<byte[]> {
  private static final int OFFSET_MAX_NUMBER_OF_TUPLES = 0;
  private static final int OFFSET_NEXT_PAGE_NUMBER = 4;
  private static final int TUPLE_COMPONENT_SIZE = 8;
  
  private int startPageNumber = 0;
  private int nextPageNumber = -1;
  private int numberOfTuplesRead = 0;
  private Storage storage;
  private int tupleSize;
  private int maxNumberOfTuples;
  private byte[] buffer;
  private String fileName;
  private int pageSize;

  public GetTupleFromRelationIterator(String fileName, int tupleSize, int startPageNumber) throws Exception {
    this.fileName = fileName;
    this.tupleSize = tupleSize;
    this.startPageNumber = startPageNumber;
    this.storage = new Storage();
    this.storage.LoadStorage(fileName);
    this.pageSize = storage.pageSize;
    this.buffer = new byte[pageSize];
  }

  public boolean hasNext() {
    return nextPageExists() || !hasReadAllTuplesOnPage();
  }

  /*
   * This function was taken from PrintRelation1
   */
  private int toInt(byte[] bytes, int offset) {
    int ret = 0;
    for (int i = 0; i < 4; i++)
    {
      ret <<= 8;
      ret |= bytes[(offset + i)] & 0xFF;
    }
    return ret;
  }

  public byte[] next() {
    if (hasReadAllTuplesOnPage()) {
      if (!nextPageExists()) {
        return null;
      }
      
      try {
    	  writeNextPageToBuffer();  
      }
      catch(Exception e) {
    	  e.printStackTrace();
    	  return null;
      }
    }
    return readTuple();
  }
  
  private boolean hasReadAllTuplesOnPage() {
	  return numberOfTuplesRead == maxNumberOfTuples;
  }
  
  private boolean nextPageExists() {
	  return nextPageNumber > 0;
  }
  
  private void writeNextPageToBuffer() throws Exception {
	  buffer = new byte[pageSize];
      storage.ReadPage(nextPageNumber, buffer);
      updatePageData();
  }
  
  private byte[] readTuple() {
	  byte[] tuple = new byte[tupleSize];
      for (int i = 0; i < tupleSize; i++) {
    	int offset = (tupleSize * numberOfTuplesRead) + i + TUPLE_COMPONENT_SIZE;
        tuple[i] = buffer[offset];
      }
      numberOfTuplesRead += 1;
      return tuple;
  }

  public void remove() {}

  public void open() throws Exception {
    storage.ReadPage(startPageNumber, buffer);
    updatePageData();
  }
  
  private void updatePageData() {
      numberOfTuplesRead = 0;
      maxNumberOfTuples = toInt(buffer, OFFSET_MAX_NUMBER_OF_TUPLES);
      nextPageNumber = toInt(buffer, OFFSET_NEXT_PAGE_NUMBER);
  }

  public void close() throws Exception {}
}
