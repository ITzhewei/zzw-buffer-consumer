package com.github.zzw.bufferconsumer.impl;

import static com.google.common.base.Suppliers.memoize;

import java.util.function.Supplier;

import com.github.zzw.bufferconsumer.BufferConsumer;


/**
 * @author zhangzhewei
 * Created on 2019-06-05
 */
public class LazyBufferConsumer<T> implements BufferConsumer<T> {

    private final Supplier<BufferConsumer<T>> factory;

    public LazyBufferConsumer(Supplier<BufferConsumer<T>> factory) {
        this.factory = memoize(factory::get);
    }

    @Override
    public void enqueue(T element) {
        BufferConsumer<T> consumer = this.factory.get();
        if (consumer != null) {
            consumer.enqueue(element);
        }
    }

    @Override
    public void doConsume() {
        BufferConsumer<T> bufferConsumer = this.factory.get();
        if (bufferConsumer != null) {
            bufferConsumer.doConsume();
        }
    }

    @Override
    public long getPendingChanges() {
        BufferConsumer<T> consumer = this.factory.get();
        if (consumer != null) {
            return consumer.getPendingChanges();
        }
        return 0L;
    }
}
