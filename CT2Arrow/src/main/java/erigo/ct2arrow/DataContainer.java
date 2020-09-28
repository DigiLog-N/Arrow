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

package erigo.ct2arrow;

import cycronix.ctlib.CTdata;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

public abstract class DataContainer {

    public String arrow_chanName;  // name of the output channel for the Arrow record batch
    public String ct_chanName;     // name of the input CT channel
    public CT2Arrow.DataType chanType;
    public FieldVector fieldVec = null;
    public Field field = null;
    public CTdata ctData = null;

    public DataContainer(String arrow_chanNameI, String ct_chanNameI, CT2Arrow.DataType chanTypeI) throws Exception {
        if ( (arrow_chanNameI == null) || (arrow_chanNameI.isEmpty()) ) {
            throw new Exception("IntDataContainer: Illegal Arrow channel name");
        }
        if ( (ct_chanNameI == null) || (ct_chanNameI.isEmpty()) ) {
            throw new Exception("IntDataContainer: Illegal CloudTurbine channel name");
        }
        arrow_chanName = arrow_chanNameI;
        ct_chanName = ct_chanNameI;
        chanType = chanTypeI;
    }

    public abstract void reset();

    public abstract void setValueCount(int recordsInBatchI);

    public abstract void addDataToVector(CTdata ctDataI,int vec_indexI,double timestampI);

}
