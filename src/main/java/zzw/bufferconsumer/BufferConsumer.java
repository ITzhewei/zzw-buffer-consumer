package zzw.bufferconsumer;

import zzw.bufferconsumer.impl.SimpleBufferConsumerBuilder;

/**
 * @author zhangzhewei
 * Created on 2019-06-04
 */
public interface BufferConsumer<E> {

    void enqueue(E element);

    void doConsume();

    long getPendingChanges();

    static <E, C> SimpleBufferConsumerBuilder<E, C> simpleBuilder() {
        return new SimpleBufferConsumerBuilder<>();
    }

}
