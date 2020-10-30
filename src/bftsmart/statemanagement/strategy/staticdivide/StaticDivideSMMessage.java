/**
 * Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package bftsmart.statemanagement.strategy.staticdivide;

import bftsmart.reconfiguration.views.View;
import bftsmart.statemanagement.ApplicationState;
import bftsmart.statemanagement.SMMessage;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class StaticDivideSMMessage extends SMMessage {

    private int stateSize;
    private int startChunk;
    private int endChunk;

    public StaticDivideSMMessage(int sender, int cid, int type, int stateSize, int startChunk, int endChunk, ApplicationState state, View view, int regency, int leader) {
        super(sender, cid, type, state, view, regency, leader);
        this.stateSize = stateSize;
        this.startChunk = startChunk;
        this.endChunk = endChunk;
    }

    public StaticDivideSMMessage() {
        super();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(stateSize);
        out.writeInt(startChunk);
        out.writeInt(endChunk);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        stateSize = in.readInt();
        startChunk = in.readInt();
        endChunk = in.readInt();
    }

    public int getStateSize() {
        return stateSize;
    }

    public int getStartStateChunk() {
        return startChunk;
    }

    public int getEndStateChunk() {
        return endChunk;
    }
}
