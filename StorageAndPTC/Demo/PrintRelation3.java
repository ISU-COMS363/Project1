package Demo;

import Demo.GetTupleFromRelationIterator;

public class PrintRelation3{
	public static void main(String args[]) throws Exception{
    System.out.println("PrintRelation3");
		System.out.println("The tuples after loading file to Relation are: ");
		GetTupleFromRelationIterator getTupleFromRelationIterator= new GetTupleFromRelationIterator("myDisk1", 31, 11);
		getTupleFromRelationIterator.open();
		while(getTupleFromRelationIterator.hasNext()){
			byte [] tuple = getTupleFromRelationIterator.next();
      String strTuple = new String(tuple);
      String name = strTuple.substring(0, 23);
      String field = strTuple.substring(23, 27);
      int salary = toInt(tuple, 27);
      if (salary >= 50000) {
          System.out.println(name + ", " +  field + ", " + salary);
      }
		}
	}


	private static int toInt(byte[] bytes, int offset) {
		  int ret = 0;
		  for (int i=0; i<4; i++) {
		    ret <<= 8;
		    ret |= (int)bytes[offset+i] & 0xFF;
		  }
		  return ret;
		}
}
