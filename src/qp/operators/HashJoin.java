/** hash join algorithm **/

package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.*;
import java.lang.*;

public class HashJoin extends Join{


	int batchsize;  //Number of tuples per out batch
	int numMatchedtuples;

	/** The following fields are useful during execution of
	 ** the NestedJoin operation
	 **/
	int leftindex;     // Index of the join attribute in left table
	int rightindex;    // Index of the join attribute in right table
	int pindex;     // Index of the join attribute in primary table
	int sindex;    // Index of the join attribute in secondary table

	String rfname;    // The file name where the right table is materialize
	String lfname;    // The file name where the left table is materialize

	Batch outbatch;   // Output buffer
	Batch pbatch;  // Buffer for primary table
	Batch sbatch; // Buffer for secondary table

	ObjectInputStream pin; // File pointer to the primary table file
	ObjectInputStream sin; // File pointer to the secondary table file

	ArrayList<ArrayList<Batch>> partitions; // Temp data structure to hold partitions before writing to disk
	Hashtable<Integer, Tuple> ht; // In-memory hash table
	ArrayList<Integer> matchedvals;
	
	int ptcurs;    // Cursor for left side buffer (inside batch, tuple index)
	int stcurs;    // Cursor for right side buffer 
	int pbcurs;    // Cursor for left side buffer (inside partition, batch index)
	int sbcurs;    // Cursor for right side buffer
	int pcurs;    // Cursor for left side buffer (partition index)

	boolean eopp;  // End of stream (primary table) is reached
	boolean eops;  // End of stream (secondary table)

	boolean lhasDup; // Whether there are duplicates in the left table
	boolean rhasDup; // Whether there are duplicates in the right table

	static int filenum=0;   // To get unique filenum for this operation

	int numleftdist; // no of distinct values of join attribute in the left table
	int numrightdist; // no of distinct values of join attribute in the right table

	int primarytable; // 1 - left table has join attribute as primary key, 0 - otherwise

	public HashJoin(Join jn){
		super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
	}

	private boolean partition(Operator base, int index, String name) {
		int table = (name.split("-")[1].compareTo("L")==0) ? 1 : 0; // 1 - Left table, 0 - otherwise
		HashSet<Integer> hs = new HashSet<Integer>();

		int pindex; // partition index
		partitions = new ArrayList<ArrayList<Batch>>();
		for (int i=0; i<numBuff; i++) {
			partitions.add(new ArrayList<Batch>());
		}

		Batch inbatch; // input buffer
		while((inbatch = (Batch)base.next()) != null) {
			for (int i=0; i<inbatch.size(); i++) {
				Tuple tuple = inbatch.elementAt(i);
				Object data = tuple.dataAt(index);
				if(data instanceof Integer) {
					pindex = ((Integer) data).intValue() % numBuff;
					if(hs.contains((Integer)data)) {
						//System.out.println("Duplicate: "+  ((Integer) data).intValue());
						if(table == 1)
							lhasDup = true;
						else
							rhasDup = true;
					}
					else
						hs.add((Integer)data);
					if(partitions.get(pindex).size() == 0) {
						outbatch = new Batch(batchsize);
						partitions.get(pindex).add(outbatch);
					}
					outbatch = partitions.get(pindex).get(partitions.get(pindex).size()-1);
					if(outbatch.isFull()) {
						outbatch = new Batch(batchsize);
						partitions.get(pindex).add(outbatch);
					}
					partitions.get(pindex).get(partitions.get(pindex).size()-1).add(tuple);
					//System.out.println(((Integer) data).intValue()+" added to Partition#"+pindex);
				}
			}
		}

		if(table == 1)
			numleftdist = hs.size();
		else
			numrightdist = hs.size();

		// Write to file 
		for (int i=0; i<numBuff; i++) {
			try{
				ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(name+"-"+i));
				for (int j=0; j<partitions.get(i).size(); j++) {
					out.writeObject(partitions.get(i).get(j));
				}
				//System.out.print("P["+i+"]:"+partitions.get(i).size()+", ");
				out.close();
			}catch(IOException io){
				System.out.println("HashJoin:writing the temporay file error");
				return false;
			}
		}
		//System.out.println("\nPartitioned "+name);
		return true;
	}

	/** During open finds the index of the join attributes
	 **  Materializes the right hand side into a file
	 **  Opens the connections
	 **/

	public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr =(Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);

		/** initialize the cursors of input buffers **/
		ptcurs = 0; stcurs =0; pbcurs=0; sbcurs=0; pcurs=0;
		numleftdist=0; numrightdist=0; numMatchedtuples=0;
		eopp=false; eops=true;
		lhasDup=false; rhasDup=false;
		matchedvals = new ArrayList<Integer>();
		
		/** Right hand side table is to be materialized
		 ** for the Hash join to perform
		 **/

		filenum++;

		if(!right.open()){
			return false;
		}else{
			rfname = "HJtemp-R-"+ String.valueOf(filenum);
			if(!partition(right, rightindex, rfname))
				return false;
			if(!right.close())
				return false;
		}
		if(!left.open()){
			return false;
		}else{
			lfname = "HJtemp-L-"+ String.valueOf(filenum);
			if(!partition(left, leftindex, lfname))
				return false;
			if(!left.close())
				return false;
		}
		//System.out.println("LHS has "+numleftdist+" values and hasDup="+lhasDup);
		//System.out.println("RHS has "+numrightdist+" values and hasDup="+rhasDup);

		if (lhasDup)
			primarytable = 0;
		else 
			primarytable = 1;

		pindex = primarytable == 1? leftindex : rightindex;
		sindex = primarytable == 1? rightindex : leftindex;

		//System.out.println((primarytable == 1? "LHS": "RHS") + " is the primary table");
		return true;
	}



	/** from input buffers selects the tuples satisfying join condition
	 ** And returns a page of output tuples
	 **/


	public Batch next(){
		//System.out.print("HashJoin:--------------------------in next----------------");
		//Debug.PPrint(con);
		//System.out.println();
		int i,j;
		if(pcurs == numBuff && eopp==true && eops==true) {
			//System.out.println(numMatchedtuples+" matched!!!");
			Collections.sort(matchedvals);
			//System.out.println(Arrays.toString(matchedvals.toArray()));
			close();
			return null;
		}

		outbatch = new Batch(batchsize);
		while(!outbatch.isFull()) {
			/* fetch new partition */
			if (pbcurs == 0 && eops == true && pcurs < numBuff) {
				try {
					if (primarytable == 1) {
						pin = new ObjectInputStream(new FileInputStream(lfname+"-"+pcurs));
					}
					else {
						pin = new ObjectInputStream(new FileInputStream(rfname+"-"+pcurs));
					}
					eopp = false;
					//System.out.println("Fetched new primary Partition#"+pcurs);
					pcurs++;
				} catch(IOException io) {	
					System.err.println("HashJoin:error in reading the primary file");
					System.exit(1);
				}
			}
			if (sbcurs==0 && eopp==false) {
				/* hash the primary table */
				ht = new Hashtable<Integer, Tuple>();
				eops = false;
				while (eopp==false && ht.size() < (numBuff-2)*batchsize) {
					try {
						if(ptcurs==0) {
							pbatch = (Batch) pin.readObject();
							//System.out.println("Hashed Batch#"+pbcurs);
							pbcurs++;
						}
						for (i=ptcurs; i<pbatch.size(); i++) {
							Tuple tuple = pbatch.elementAt(i);
							ht.put((Integer) tuple.dataAt(pindex), tuple);
							//System.out.println("tuple#"+i+" of id:"+(int)tuple.dataAt(pindex));
							if (ht.size() >= (numBuff-2)*batchsize) {
								//System.out.println("Hash table size reaches memory limit");
								break;
							}
						}
						ptcurs = 0;
					}
					catch(EOFException e) {
						try{
							pin.close();
						}catch (IOException io){
							System.out.println("NestedJoin:Error in temporary file reading");
						}
						//System.out.println("Reached end of partition of primary table");
						eopp=true;		
						pbcurs=0;
					}
					catch(ClassNotFoundException c) {
						System.out.println("HashJoin:Some error in deserialization ");
						System.exit(1);
					}
					catch(IOException io){
						System.out.println("HashJoin:temporary file reading error");
						System.exit(1);
					}
				}
				//System.out.println("Hashed "+ht.size()+" tuples");
			}

			while(eops == false) {
				/* read secondary table, hash and output if matches */
				if (sbcurs==0) {
					try {
						if (primarytable == 1) {
							sin = new ObjectInputStream(new FileInputStream(rfname+"-"+(pcurs-1)));
						}
						else {
							sin = new ObjectInputStream(new FileInputStream(lfname+"-"+(pcurs-1)));
						}
						//System.out.println("Fetched new secondary Partition#"+(pcurs-1));
					} catch(IOException io) {	
						System.err.println("HashJoin:error in reading the secondary file");
						System.exit(1);
					}
				}
				
				try {
					if (stcurs==0 && eops==false) {
						sbatch = (Batch) sin.readObject();
						sbcurs++;
					}

					for (j=stcurs; j<sbatch.size(); j++) {
						Tuple tuple = sbatch.elementAt(j);
						//System.out.println("Comparing with tuple#"+j+" of id:"+(int) tuple.dataAt(sindex));
						if (ht.containsKey((Integer)tuple.dataAt(sindex))) {
							Tuple outtuple = ht.get((Integer)tuple.dataAt(sindex)).joinWith(tuple);
							
							//Debug.PPrint(outtuple);
							//System.out.println();
							outbatch.add(outtuple);
							//System.out.println("Matched");
							numMatchedtuples++;
							matchedvals.add((Integer) tuple.dataAt(sindex));
						}
						if(outbatch.isFull()){
							if(j==sbatch.size()-1){ // reached end of batch
								stcurs = 0;
							}
							else {
								stcurs = j+1;
							}
							//System.out.println("Returning Output Batch");
							//System.out.println("eopp:"+eopp+", eops:"+eops+", pcurs:"+pcurs+", pbcurs:"+pbcurs+", ptcurs:"+ptcurs+", stcurs:"+stcurs);
							return outbatch;
						}
					}
					stcurs = 0;
				}
				catch(EOFException e) {
					try{
						sin.close();
					}catch (IOException io){
						System.out.println("NestedJoin:Error in temporary file reading");
					}
					//System.out.println("Reached end of partition of secondary table");
					eops=true;	
					sbcurs=0;
					if(pcurs == numBuff && eopp==true) {
						return outbatch;
					}
				}
				catch(ClassNotFoundException c) {
					System.out.println("HashJoin:Some error in deserialization ");
					System.exit(1);
				}
				catch(IOException io){
					System.out.println("HashJoin:temporary file reading error");
					System.exit(1);
				}
			}
		}	
		return outbatch;
	}



	/** Close the operator */
	public boolean close(){
		for (int i=0; i<numBuff; i++) {
			File f = new File(rfname+"-"+i);
			f.delete();
			f = new File(lfname+"-"+i);
			f.delete();
		}
		return true;
	}
}