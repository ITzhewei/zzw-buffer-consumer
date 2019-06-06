package zzw.bufferconsumer.impl;

import static com.github.phantomthief.util.MoreSuppliers.lazy;

import java.util.function.Supplier;

import com.github.phantomthief.util.MoreSuppliers;

import zzw.bufferconsumer.BufferConsumer;

/**
 * @author zhangzhewei
 * Created on 2019-06-05
 */
public class LazyBufferConsumer<E> implements BufferConsumer<E> {

    private final MoreSuppliers.CloseableSupplier<BufferConsumer<E>> factory;

    public LazyBufferConsumer(Supplier<BufferConsumer<E>> factory) {
        this.factory = lazy(factory);
    }

    @Override
    public void enqueue(E element) {
        BufferConsumer<E> consumer = this.factory.get();
        if (consumer != null) {
            consumer.enqueue(element);
        }
    }

    @Override
    public void doConsume() {
        this.factory.ifPresent(BufferConsumer::doConsume);
    }

    @Override
    public long getPendingChanges() {
        return this.factory.map(BufferConsumer::getPendingChanges).orElse(0L);
    }
}
