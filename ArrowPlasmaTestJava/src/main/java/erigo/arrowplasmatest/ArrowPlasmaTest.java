/*
Copyright 2020 Erigo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*

ArrowPlasmaTest

John Wilson, Erigo Technologies

Simple Java Arrow test using Plasma in-memory object store
Based on code snippets found at https://arrow.apache.org/docs/java/ipc.html

*/

package erigo.arrowplasmatest;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.*;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.*;

public class ArrowPlasmaTest {
	
	public static void main(String[] arg) {
		new ArrowPlasmaTest(arg);
	}
	
	public ArrowPlasmaTest(String[] arg) {

		// Number of samples in each channel per batch
		int batchSize = 10;

		// Write out some bytes to Plasma
		System.loadLibrary("plasma_java");
		PlasmaClient client = new PlasmaClient("/tmp/plasma", "", 0);
		byte[] id = new byte[20];
		Arrays.fill(id, (byte) 1);
		byte[] value = new byte[20];
		Arrays.fill(value, (byte) 97);
		client.put(id, value, null);
		//client.seal(id);

		// Write a record batch to Plasma
		RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
		IntVector intVector = new IntVector("int",allocator);
		VarCharVector varCharVector = new VarCharVector("varchar", allocator);
		BitVector bitVector = new BitVector("boolean", allocator);
		for (int i = 0; i < batchSize; i++) {
			intVector.setSafe(i,i);
			varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
			bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
		}
		intVector.setValueCount(batchSize);
		varCharVector.setValueCount(batchSize);
		bitVector.setValueCount(batchSize);
		
		List<Field> fields = Arrays.asList(intVector.getField(), varCharVector.getField(), bitVector.getField());
		List<FieldVector> vectors = Arrays.asList(intVector, varCharVector, bitVector);
		VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ArrowStreamWriter writer = new ArrowStreamWriter(root, /*DictionaryProvider=*/null, Channels.newChannel(out));
		try {
			writer.start();
			root.setRowCount(batchSize);
			writer.writeBatch();
		} catch (IOException ioe) {
			System.err.println(ioe);
		}
		// Write out new batches of data
		for (int i=0; i<2; ++i) {
			// Fill the vectors with data
			intVector.reset();
			varCharVector.reset();
			bitVector.reset();
			for (int j = 0; j <= batchSize; ++j) {
				intVector.setSafe(j,j*(i+1));
				varCharVector.setSafe(j, ("i = " + i + ", j = " + j).getBytes(StandardCharsets.UTF_8));
				bitVector.setSafe(j, i % 2 == 0 ? 0 : 1);
			}
			try {
				root.setRowCount(batchSize);
				writer.writeBatch();
			} catch (IOException ioe) {
				System.err.println(ioe);
			}
		}
		//
		// Write out to Plasma
		//
		byte[] nextID = new byte[20];
		Arrays.fill(nextID, (byte) 2);
		byte[] recordAsBytes = out.toByteArray();
		System.err.println("the record batch contains " + recordAsBytes.length + " bytes");
		// We could create a buffer in Plasma and then write into that buffer;
		// but the following call to client.put will do this
		// ByteBuffer plasmaBuf = client.create(nextID,recordAsBytes.length,null);
		client.put(nextID,recordAsBytes,null);
		// The client.put call above automatically seals the object in Plasma, don't do it again
		// client.seal(nextID);
		//
		// Close the ArrowFileWriter and ByteArrayOutputStream
		//
		try {
			writer.end();
			out.close();
		} catch (IOException ioe) {
			System.err.println(ioe);
		}

	}
	
} //end class ArrowPlasmaTest
