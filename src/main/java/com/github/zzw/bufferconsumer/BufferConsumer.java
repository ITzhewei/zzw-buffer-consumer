package com.github.zzw.bufferconsumer;


import com.github.zzw.bufferconsumer.impl.SimpleBufferConsumerBuilder;

/**
 * @author zhangzhewei
 * Created on 2019-06-04
 */
public interface BufferConsumer<T> {

    void enqueue(T element);

    void doConsume();

    long getPendingChanges();

    static <T, C> SimpleBufferConsumerBuilder<T, C> simpleBuilder() {
        return new SimpleBufferConsumerBuilder<>();
    }

}
