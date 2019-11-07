package com.nanosai.netops.iap;

import com.nanosai.rionops.rion.RionFieldTypes;
import com.nanosai.rionops.rion.read.RionReader;
import com.nanosai.rionops.rion.write.RionWriter;
import com.nanosai.netops.codec.RionCodec;

/**
 * Created by jjenkov on 26-11-2016.
 */
public class IapMessageBase implements RionCodec {


    // receiver system id
    public byte[] receiverNodeIdSource = null;
    public int    receiverNodeIdOffset = 0;
    public int    receiverNodeIdLength = 0;

    // semantic protocol id
    public byte[] semanticProtocolIdSource = null;
    public int    semanticProtocolIdOffset = 0;
    public int    semanticProtocolIdLength = 0;

    // semantic protocol version
    public byte[] semanticProtocolVersionSource = null;
    public int    semanticProtocolVersionOffset = 0;
    public int    semanticProtocolVersionLength = 0;

    // message type
    public byte[] messageTypeSource = null;
    public int    messageTypeOffset = 0;
    public int    messageTypeLength = 0;


    public IapMessageBase() {
    }

    public IapMessageBase(byte[] receiverNodeIdSource, byte[] semanticProtocolIdSource, byte[] semanticProtocolVersionSource, byte[] messageTypeSource) {
        this.receiverNodeIdSource = receiverNodeIdSource;
        this.receiverNodeIdLength = receiverNodeIdSource.length;
        this.semanticProtocolIdSource = semanticProtocolIdSource;
        this.semanticProtocolIdLength = semanticProtocolIdSource.length;
        this.semanticProtocolVersionSource = semanticProtocolVersionSource;
        this.semanticProtocolVersionLength = semanticProtocolVersionSource.length;
        this.messageTypeSource = messageTypeSource;
        this.messageTypeLength = messageTypeSource.length;
    }

    public void setReceiverNodeId(byte[] receiverNodeId){
        this.receiverNodeIdSource = receiverNodeId;
        this.receiverNodeIdOffset = 0;
        this.receiverNodeIdLength = receiverNodeId.length;
    }

    public long getReceiverNodeIdAsLong() {
        return getAsLong(this.receiverNodeIdSource, this.receiverNodeIdOffset, this.receiverNodeIdLength);
    }

    public long getReceiverNodeIdAsInt() {
        return getAsInt(this.receiverNodeIdSource, this.receiverNodeIdOffset, this.receiverNodeIdLength);
    }

    public long getReceiverNodeIdAsShort() {
        return getAsShort(this.receiverNodeIdSource, this.receiverNodeIdOffset, this.receiverNodeIdLength);
    }

    public long getAsLong(byte[] source, int offset, int length){
        long value = 0;
        for(int i=0; i<length; i++){
            value <<=8;
            value |= 255 & source[offset + i];
        }
        return value;
    }

    public int getAsInt(byte[] source, int offset, int length){
        int value = 0;
        for(int i=0; i<length; i++){
            value <<=8;
            value |= 255 & source[offset + i];
        }
        return value;
    }

    public short getAsShort(byte[] source, int offset, int length){
        short value = 0;
        for(int i=0; i<length; i++){
            value <<=8;
            value |= 255 & source[offset + i];
        }
        return value;
    }

    public void setSemanticProtocolId(byte[] semanticProtocolId){
        this.semanticProtocolIdSource = semanticProtocolId;
        this.semanticProtocolIdOffset = 0;
        this.semanticProtocolIdLength = semanticProtocolId.length;
    }

    public long getSemanticProtocolIdAsLong() {
        return getAsLong(this.semanticProtocolIdSource, this.semanticProtocolIdOffset, this.semanticProtocolIdLength);
    }

    public int getSemanticProtocolIdAsInt() {
        return getAsInt(this.semanticProtocolIdSource, this.semanticProtocolIdOffset, this.semanticProtocolIdLength);
    }

    public short getSemanticProtocolIdAsShort() {
        return getAsShort(this.semanticProtocolIdSource, this.semanticProtocolIdOffset, this.semanticProtocolIdLength);
    }

    public void setSemanticProtocolVersion(byte[] semanticProtocolVersion){
        this.semanticProtocolVersionSource = semanticProtocolVersion;
        this.semanticProtocolVersionOffset = 0;
        this.semanticProtocolVersionLength = semanticProtocolVersion.length;
    }

    public long getSemanticProtocolVersionAsLong() {
        return getAsLong(this.semanticProtocolVersionSource, this.semanticProtocolVersionOffset, this.semanticProtocolVersionLength);
    }

    public int getSemanticProtocolVersionAsInt() {
        return getAsInt(this.semanticProtocolVersionSource, this.semanticProtocolVersionOffset, this.semanticProtocolVersionLength);
    }

    public short getSemanticProtocolVersionAsShort() {
        return getAsShort(this.semanticProtocolVersionSource, this.semanticProtocolVersionOffset, this.semanticProtocolVersionLength);
    }

    public void setMessageType(byte[] messageType){
        this.messageTypeSource = messageType;
        this.messageTypeOffset = 0;
        this.messageTypeLength = messageType.length;
    }

    public long getMessageTypeAsLong() {
        return getAsLong(this.messageTypeSource, this.messageTypeOffset, this.messageTypeLength);
    }

    public int getMessageTypeAsInt() {
        return getAsInt(this.messageTypeSource, this.messageTypeOffset, this.messageTypeLength);
    }

    public short getMessageTypeAsShort() {
        return getAsShort(this.messageTypeSource, this.messageTypeOffset, this.messageTypeLength);
    }


    @Override
    public void read(RionReader reader) {
        readReceiverNodeId(reader);
        readSemanticProtocolId(reader);
        readSemanticProtocolVersion(reader);
        readMessageType(reader);
    }

    private void readReceiverNodeId(RionReader reader) {
        if(reader.fieldType == RionFieldTypes.KEY_SHORT){
            if(isIdKey(reader, IapMessageKeys.RECEIVER_NODE_ID)) {
                reader.nextParse();

                if(reader.fieldType == RionFieldTypes.BYTES){
                    this.receiverNodeIdSource = reader.source;
                    this.receiverNodeIdOffset = reader.index;
                    this.receiverNodeIdLength = reader.fieldLength;
                }
                reader.nextParse(); //move to next field after receiver system id.
            }
        }
    }

    private void readSemanticProtocolId(RionReader reader) {
        if(reader.fieldType == RionFieldTypes.KEY_SHORT){
            if(isIdKey(reader, IapMessageKeys.SEMANTIC_PROTOCOL_ID)) {
                reader.nextParse();

                if(reader.fieldType == RionFieldTypes.BYTES){
                    this.semanticProtocolIdSource = reader.source;
                    this.semanticProtocolIdOffset = reader.index;
                    this.semanticProtocolIdLength = reader.fieldLength;
                }
                reader.nextParse(); //move to next field after receiver system id.
            }
        }
    }

    private void readSemanticProtocolVersion(RionReader reader) {
        if(reader.fieldType == RionFieldTypes.KEY_SHORT){
            if(isIdKey(reader, IapMessageKeys.SEMANTIC_PROTOCOL_VERSION)) {
                reader.nextParse();

                if(reader.fieldType == RionFieldTypes.BYTES){
                    this.semanticProtocolVersionSource = reader.source;
                    this.semanticProtocolVersionOffset = reader.index;
                    this.semanticProtocolVersionLength = reader.fieldLength;
                }
                reader.nextParse(); //move to next field after receiver system id.
            }
        }
    }

    private void readMessageType(RionReader reader) {
        if(reader.fieldType == RionFieldTypes.KEY_SHORT){
            if(isIdKey(reader, IapMessageKeys.MESSAGE_TYPE)) {
                reader.nextParse();

                if(reader.fieldType == RionFieldTypes.BYTES){
                    this.messageTypeSource = reader.source;
                    this.messageTypeOffset = reader.index;
                    this.messageTypeLength = reader.fieldLength;
                }
                reader.nextParse(); //move to next field after receiver system id.
            }
        }
    }

    private static boolean isIdKey(RionReader reader, int singleByteKeyValue){
        return reader.fieldLength == 1 && reader.source[reader.index] == singleByteKeyValue;
    }


    @Override
    public void write(RionWriter writer) {
        writeReceiverNodeId         (writer);
        writeSemanticProtocolId     (writer);
        writeSemanticProtocolVersion(writer);
        writeMessageType            (writer);
    }

    /*
    public static void writeReceiverNodeIdCode(RionWriter writer, int receiverSystemIdCode){
        writer.writeKeyShort(receiverNodeIdCodeKey);
        writer.writeInt64   (receiverSystemIdCode);
    }
    */

    public void writeReceiverNodeId(RionWriter writer){
        writer.writeKeyShort((byte) IapMessageKeys.RECEIVER_NODE_ID);
        writer.writeBytes   (this.receiverNodeIdSource, this.receiverNodeIdOffset, this.receiverNodeIdLength);
    }

    /*
    public static void writeSemanticProtocolIdCode(RionWriter writer, int semanticProtocolIdCode){
        writer.writeKeyShort(semanticProtocolIdCodeKey);
        writer.writeInt64   (semanticProtocolIdCode);
    }
    */

    public void writeSemanticProtocolId(RionWriter writer){
        writer.writeKeyShort((byte) IapMessageKeys.SEMANTIC_PROTOCOL_ID);
        writer.writeBytes   (this.semanticProtocolIdSource, this.semanticProtocolIdOffset, this.semanticProtocolIdLength);
    }

    /*
    public static void writeSemanticProtocolVersionCode(RionWriter writer, int semanticProtocolVersionCode){
        writer.writeKeyShort(semanticProtocolVersionCodeKey);
        writer.writeInt64   (semanticProtocolVersionCode);
    }
    */

    public void writeSemanticProtocolVersion(RionWriter writer){
        writer.writeKeyShort((byte) IapMessageKeys.SEMANTIC_PROTOCOL_VERSION);
        writer.writeBytes   (this.semanticProtocolVersionSource, this.semanticProtocolVersionOffset, this.semanticProtocolVersionLength);
    }

    /*
    public static void writeMessageTypeCode(RionWriter writer, int messageType){
        writer.writeKeyShort(messageTypeCodeKey);
        writer.writeInt64   (messageType);
    }
    */

    public void writeMessageType(RionWriter writer){
        writer.writeKeyShort((byte) IapMessageKeys.MESSAGE_TYPE);
        writer.writeBytes   (this.messageTypeSource, this.messageTypeOffset, this.messageTypeLength);
    }

    public boolean equalsReceiverNodeId(byte[] nodeId) {
        return equals(this.receiverNodeIdSource, this.receiverNodeIdOffset, this.receiverNodeIdLength,
                      nodeId, 0, nodeId.length);
    }

    public boolean equalsSemanticProtocolId(byte[] semanticProtocolId) {
        return equals(this.semanticProtocolIdSource, this.semanticProtocolIdOffset, this.semanticProtocolIdLength,
                      semanticProtocolId, 0, semanticProtocolId.length);
    }

    public boolean equalsSemanticProtocolVersion(byte[] semanticProtocolVersion) {
        return equals(this.semanticProtocolVersionSource, this.semanticProtocolVersionOffset, this.semanticProtocolVersionLength,
                semanticProtocolVersion, 0, semanticProtocolVersion.length);
    }

    public boolean equalsMessageType(byte[] messageType) {
        return equals(this.messageTypeSource, this.messageTypeOffset, this.messageTypeLength,
                messageType, 0, messageType.length);
    }

    public static boolean equals(byte[] source1, int offset1, int length1, byte[] source2, int offset2, int length2){
        if(length1 != length2){
            return false;
        }

        for(int i=0; i<length1; i++){
            if(source1[offset1 + i] != source2[offset2 + i]){
                return false;
            }
        }
        return true;
    }
}
