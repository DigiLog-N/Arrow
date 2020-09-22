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

package erigo.ct2plasma;

import cycronix.ctlib.CTdata;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;

public class StringDataContainer extends DataContainer {

    public VarCharVector vec;

    public StringDataContainer(String arrow_chanNameI, String ct_chanNameI, RootAllocator allocatorI) throws Exception {
        super(arrow_chanNameI, ct_chanNameI, CT2Plasma.DataType.STRING_DATA);
        vec = new VarCharVector(arrow_chanName,allocatorI);
        vec.allocateNew(100);
        fieldVec = vec;
        field = vec.getField();
    }

    public void reset() {
        vec.reset();
    }

    public void setValueCount(int recordsInBatchI) {
        vec.setValueCount(recordsInBatchI);
    }

    //
    // Add a single datapoint to our vector.
    // Look through the given CTdata for a datapoint whose time matches the given timestamp.
    // If a matching timestamp is found, add that data to our vector. Otherwise, add
    // null to the vector at this index.
    //
    public void addDataToVector(CTdata ctDataI,int vec_indexI,double timestampI) {
        if (ctDataI == null) {
            vec.setSafe(vec_indexI, "n/a".getBytes(StandardCharsets.UTF_8));
            return;
        }
        double[] times = ctDataI.getTime();
        byte[][] data = ctDataI.getData();
        int data_index = -1;
        for (int i = 0; i<times.length; ++i) {
            if ( Math.abs(times[i] - timestampI) < 0.0001 ) {
                // We've got a match!
                data_index = i;
                break;
            }
        }
        if (data_index == -1) {
            // Store null at this index in the vector
            System.err.println("Channel " + arrow_chanName + ": didn't find timestamp " + timestampI + " in the given CTdata structure; store null");
            // NB: There's no "setSafe" function which includes the "isSet" argument for a VarCharVector
            // Can either risk it and call the "setNull" function (which won't be good if our index is over the limit
            // or we can just store a "n/a" string
            // vec.setNull(indexI);
            vec.setSafe(vec_indexI, "n/a".getBytes(StandardCharsets.UTF_8));
        } else {
            vec.setSafe(vec_indexI, data[data_index]);
        }
    }

}
