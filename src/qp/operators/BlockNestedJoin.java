package qp.operators;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Vector;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

public class BlockNestedJoin extends Join{

	int batchsize;  //Number of tuples per out batch

	/** The following fields are useful during execution of
	 ** the BlockNestedJoin operation
	 **/
	int leftindex;     // Index of the join attribute in left table
	int rightindex;    // Index of the join attribute in right table

	String rfname;    // The file name where the right table is materialize

	static int filenum=0;   // To get unique filenum for this operation

	Batch outbatch;   // Output buffer
	Batch leftbatch;  // Buffer for left input stream
	Batch rightbatch;  // Buffer for right input stream
	ObjectInputStream in; // File pointer to the right hand materialized file

	int lcurs;    // Cursor for left side buffer
	int rcurs;    // Cursor for right side buffer
	int pcurs; // page index of the block
	boolean eosl;  // Whether end of stream (left table) is reached
	boolean eosr;  // End of stream (right table)

	HashMap<Integer,Vector<Tuple>> hashTable;
	int MaxNumTuplesAllowedInHashTable;
	int currentNumTuplesInHashTable;
	Vector<Tuple> output = new Vector<>();

	public BlockNestedJoin(Join jn){
		super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
		hashTable = new HashMap<>();
		currentNumTuplesInHashTable = 0;

	}

	
	/** During open finds the index of the join attributes
	 **  Materializes the right hand side into a file
	 **  Opens the connections
	 **/

	public boolean open() {

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;
		if(Batch.getPageSize() < tuplesize) {
			System.out.println("BlockNestedJoin:page size is smaller than tuple size");
			System.exit(1);
		}

		currentNumTuplesInHashTable = 0;
		MaxNumTuplesAllowedInHashTable = (numBuff - 2) * batchsize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr =(Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		
		Batch rightpage;
		
		/** initialize the cursors of input buffers **/
		lcurs = 0; rcurs =0;
		eosl=false;
		
		/** because right stream is to be repetitively scanned
		 ** if it reached end, we have to start new scan
		 **/
		eosr=true;

		/** Right hand side table is to be materialized
		 ** for the Block Nested join to perform
		 **/
		if(!right.open()){
			return false;
		}else{
			/** If the right operator is not a base table then
			 ** Materialize the intermediate result from right
			 ** into a file
			 **/

			//if(right.getOpType() != OpType.SCAN){
			filenum++;
			rfname = "NJtemp-" + String.valueOf(filenum);
			try{
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
				while( (rightpage = right.next()) != null){
					out.writeObject(rightpage);
				}
				out.close();
			}catch(IOException io){
				System.out.println("BlockNestedJoin:writing the temporay file error");
				return false;
			}
			//}
			if(!right.close())
				return false;
		}
		if(left.open())
			return true;
		else
			return false;

	}

	/** from input buffers selects the tuples satisfying join condition
	 ** And returns a page of output tuples
	 **/
	public Batch next(){
		//System.out.print("BlockNestedJoin:--------------------------in next----------------");
		//Debug.PPrint(con);
		//System.out.println();

		outbatch = new Batch(batchsize);
		while(output.isEmpty() == false)
		{
			outbatch.add(output.elementAt(0));
			output.removeElementAt(0);
			if(outbatch.isFull())
				return outbatch;
		}

		int i,j;
		if(eosl){
			close();
			return null;
		}

		while(!outbatch.isFull())
		{
			if(eosr==true) //Start populating hashTable from scratch
			{
				hashTable.clear();
				currentNumTuplesInHashTable = 0;

				while(currentNumTuplesInHashTable != MaxNumTuplesAllowedInHashTable)
				{
					if(leftbatch != null)
					{
						if(leftbatch.size() == 0){
							leftbatch = null;
						}
						//read remaining tuples from leftbatch if exists
						while(leftbatch != null && currentNumTuplesInHashTable != MaxNumTuplesAllowedInHashTable)
						{
							Tuple tuple = leftbatch.elementAt(lcurs);
							int key = tuple.dataAt(leftindex).hashCode();
							if(hashTable.get(key) == null)
								hashTable.put(key, new Vector<Tuple>());

							//put tuple in hashtable
							hashTable.get(key).add(tuple);
							currentNumTuplesInHashTable++;
							lcurs++;

							if(lcurs == leftbatch.size()){
								lcurs = 0;
								leftbatch = null;
								break;
							}
						}
					}

					//Read block*batchsize to hashTable
					if(leftbatch == null){
						leftbatch = (Batch) left.next();
					}

					if(leftbatch == null && currentNumTuplesInHashTable == 0) //nothing left to process
					{
						if(output.isEmpty())
						{
							return outbatch.size() == 0 ? null : outbatch;
						}

						while(output.isEmpty() == false)
						{
							outbatch.add(output.elementAt(0));
							output.removeElementAt(0);
							if(outbatch.isFull())
								return outbatch;
						}
					}
					else if(leftbatch == null && currentNumTuplesInHashTable != 0)
					{
						break;
					}
					else //if(leftbatch != null && currentNumTuplesInHashTable != 0)
					{
						//Read as much as possible to hashtable
						while(currentNumTuplesInHashTable != MaxNumTuplesAllowedInHashTable)
						{
							if(leftbatch.size() == 0)
								break;
							Tuple tuple = leftbatch.elementAt(lcurs);
							int key = tuple.dataAt(leftindex).hashCode();
							if(hashTable.get(key) == null)
								hashTable.put(key, new Vector<Tuple>());

							//put tuple in hashtable
							hashTable.get(key).add(tuple);
							currentNumTuplesInHashTable++;
							lcurs++;
							if(lcurs == leftbatch.size())
							{
								lcurs = 0;
								leftbatch = null;
								break;
							}

						}
						//Process on hashtable
					}
				}//while(currentNumTuplesInHashTable != MaxNumTuplesAllowedInHashTable)

				/** Whenver a new hashtable made , we have to start the
				 ** scanning of entire right table
				 **/
				try{

					in = new ObjectInputStream(new FileInputStream(rfname));
					eosr=false; //ENABLE SCANNING OF RIGHT
				}
				catch(IOException io)
				{
					System.err.println("BlockJoin:error in reading the file");
					System.exit(1);
				}
			} //if(eosr==true)

			while(eosr==false)
			{
				try
				{
					if(rcurs==0)
					{ //changed rcurs==0 && lcurs==0
						rightbatch = (Batch) in.readObject();
					}

					for(j=rcurs;j<rightbatch.size();j++)
					{

						Tuple righttuple = rightbatch.elementAt(j);
						int key = righttuple.dataAt(rightindex).hashCode();

						if(hashTable.get(key) == null)
							continue;
						Vector<Tuple> leftTuples = hashTable.get(key);

						for(int x = 0; x < leftTuples.size(); x++)
						{
							Tuple lefttuple = leftTuples.elementAt(x);
							if(lefttuple.checkJoin(righttuple,leftindex,rightindex))
							{
								Tuple outtuple = lefttuple.joinWith(righttuple);
								output.add(outtuple);	

							}//if joins?
						}
					}
					rcurs =0;
					while(output.isEmpty() == false)
					{
						outbatch.add(output.elementAt(0));
						output.removeElementAt(0);
						if(outbatch.isFull())
							return outbatch;
					}
				}
				catch(EOFException e)
				{
					try{
						in.close();
					}catch (IOException io){
						System.out.println("BlockNestedJoin:Error in temporary file reading");
					}
					eosr=true;
				}
				catch(ClassNotFoundException c){
					System.out.println("BlockNestedJoin:Some error in deserialization ");
					System.exit(1);
				}
				catch(IOException io){
					System.out.println("BlockNestedJoin:temporary file reading error");
					System.exit(1);
				}
			}//while(eosr==false)
		}//while outbatch is full
		return outbatch;
	}


	/** Close the operator */
	public boolean close(){

		File f = new File(rfname);
		f.delete();
		return true;

	}
}