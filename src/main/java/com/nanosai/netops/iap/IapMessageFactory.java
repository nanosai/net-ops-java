package com.nanosai.netops.iap;

import com.nanosai.memops.bytes.BytesAllocatorAutoDefrag;
import com.nanosai.memops.objects.IObjectFactory;
import com.nanosai.memops.objects.ObjectPool;

/**
 * Created by jjenkov on 04/02/2018.
 */
public class IapMessageFactory implements IObjectFactory {

    private BytesAllocatorAutoDefrag bytesAllocator;

    public IapMessageFactory(BytesAllocatorAutoDefrag bytesAllocator){
        this.bytesAllocator = bytesAllocator;
    }

    @Override
    public Object instance(ObjectPool objectPool) {
        return new IapMessage(this.bytesAllocator, objectPool);
    }
}
