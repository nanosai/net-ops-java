package com.nanosai.netops.codec;

import com.nanosai.rionops.rion.read.RionReader;
import com.nanosai.rionops.rion.write.RionWriter;

/**
 * An RionCodec implementation is capable of reading and writing some data structure to and from the ION data format.
 * Such implementations are often simple value objects representing full or partial IAP messages.
 *
 */
public interface RionCodec {

    public void read(RionReader reader);
    public void write(RionWriter writer);


}
