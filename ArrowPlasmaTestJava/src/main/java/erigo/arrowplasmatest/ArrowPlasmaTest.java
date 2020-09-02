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

import java.io.IOException;
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.arrow.memory.*;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.*;

public class ArrowPlasmaTest {
	
	public static void main(String[] arg) {
		new ArrowPlasmaTest(arg);
	}
	
	public ArrowPlasmaTest(String[] arg) {
		
		PlasmaClient client = new PlasmaClient("/tmp/plasma", "", 0);
		
		RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
		IntVector intVector = new IntVector("int",allocator);
		VarCharVector varCharVector = new VarCharVector("varchar", allocator);
		BitVector bitVector = new BitVector("boolean", allocator);
		for (int i = 0; i < 10; i++) {
			intVector.setSafe(i,i);
			varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
			bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
		}
		intVector.setValueCount(10);
		varCharVector.setValueCount(10);
		bitVector.setValueCount(10);
		
		List<Field> fields = Arrays.asList(intVector.getField(), varCharVector.getField(), bitVector.getField());
		List<FieldVector> vectors = Arrays.asList(intVector, varCharVector, bitVector);
		VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

		// this is a try-with-resource block
		try (FileOutputStream fos = new FileOutputStream(".\\test.arrow");
			 // Make the writer
             ArrowFileWriter fileWriter = new ArrowFileWriter(root, null, Channels.newChannel(fos))) {
			fileWriter.start();
			// Write out first batch of data
			fileWriter.writeBatch();
			// Write out new batches of data
			for (int i=0; i<2; ++i) {
				IntVector childVector_int = (IntVector)root.getVector("int");
				VarCharVector childVector_str = (VarCharVector)root.getVector("varchar");
				BitVector childVector_bool = (BitVector)root.getVector("boolean");
				childVector_int.reset();
				childVector_str.reset();
				childVector_bool.reset();
				for (int j = 0; j <= 10; ++j) {
					childVector_int.setSafe(j,j*(i+1));
					childVector_str.setSafe(j, ("i = " + i + ", j = " + j).getBytes(StandardCharsets.UTF_8));
					childVector_bool.setSafe(j, i % 2 == 0 ? 0 : 1);
				}
				childVector_int.setValueCount(10);
				childVector_str.setValueCount(10);
				childVector_bool.setValueCount(10);
				fileWriter.writeBatch();
			}
			// Close the ArrowFileWriter
			fileWriter.end();
		} catch (IOException ioe) {
			System.err.println(ioe);
		}

	}
	
} //end class ArrowPlasmaTest
