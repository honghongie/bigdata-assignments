/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.honghongie;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.array.ArrayListOfFloatsWritable;

/**
 * Representation of a graph node for PageRanks. 
 *
 * @author Jimmy Lin
 * @author Michael Schatz
 * multiple pagerank values
 * @quthor Lingzi Hong
 */

public class PageRankNode implements Writable {
  public static enum Type {
    Complete((byte) 0),  // PageRank mass and adjacency list.
    Mass((byte) 1),      // PageRank mass only.
    Structure((byte) 2); // Adjacency list only.

    public byte val;

    private Type(byte v) {
      this.val = v;
    }
  };

	private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

	private Type type;
	private int nodeid;
	private ArrayListOfFloatsWritable pageranks;
	private ArrayListOfIntsWritable adjacenyList;

	public PageRankNode() {}

	public ArrayListOfFloatsWritable getPageRank() {
		return pageranks;
	}

	public void setPageRank(ArrayListOfFloatsWritable pageranklist) {

		//if (pageranks==null)
		//{
		//	pageranks=new ArrayListOfFloatsWritable();
		//	System.out.println("null pagerank object!");
		//}
		
		this.pageranks = pageranklist;
	}

	public int getNodeId() {
		return nodeid;
	}

	public void setNodeId(int n) {
		this.nodeid = n;
	}

	public ArrayListOfIntsWritable getAdjacenyList() {
		return adjacenyList;
	}

	public void setAdjacencyList(ArrayListOfIntsWritable list) {
		this.adjacenyList = list;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * Deserializes this object.
	 *
	 * @param in source for raw byte representation
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		int b = in.readByte();
		type = mapping[b];
		nodeid = in.readInt();

		if (type.equals(Type.Mass)) {
			pageranks = new ArrayListOfFloatsWritable();
			pageranks.readFields(in);
			return;
		}

		if (type.equals(Type.Complete)) {
			pageranks = new ArrayListOfFloatsWritable();
			pageranks.readFields(in);
		}

		adjacenyList = new ArrayListOfIntsWritable();
		adjacenyList.readFields(in);
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeByte(type.val);
		out.writeInt(nodeid);

		if (type.equals(Type.Mass)) {
			pageranks.write(out);
			return;
		}

		if (type.equals(Type.Complete)) {
			pageranks.write(out);
		}

		adjacenyList.write(out);
	}

	@Override
	public String toString() {
		return String.format("{%d %s %s}",
				nodeid, (pageranks.toString()), (adjacenyList == null ? "[]" : adjacenyList.toString(10)));
	}


  /**
   * Returns the serialized representation of this object as a byte array.
   *
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

  /**
   * Creates object from a <code>DataInput</code>.
   *
   * @param in source for reading the serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(DataInput in) throws IOException {
    PageRankNode m = new PageRankNode();
    m.readFields(in);

    return m;
  }

  /**
   * Creates object from a byte array.
   *
   * @param bytes raw serialized representation
   * @return newly-created object
   * @throws IOException
   */
  public static PageRankNode create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
