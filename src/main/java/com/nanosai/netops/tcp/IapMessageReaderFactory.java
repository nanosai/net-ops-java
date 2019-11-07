package com.nanosai.netops.tcp;

/**
 * Created by jjenkov on 27-10-2015.
 */
public class IapMessageReaderFactory implements IMessageReaderFactory {

    @Override
    public IMessageReader createMessageReader() {
        return new IapMessageReader();
    }

}
