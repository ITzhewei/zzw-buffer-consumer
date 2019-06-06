package zzw.bufferconsumer.impl;

import static java.lang.System.currentTimeMillis;
import static zzw.bufferconsumer.impl.SimpleBufferConsumerBuilder.DEFAULT_NEXT_TRIGGER_PERIOD;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.util.ThrowableConsumer;

import zzw.bufferconsumer.BufferConsumer;
import zzw.bufferconsumer.ConsumerStrategy;

/**
 * @author zhangzhewei
 * Created on 2019-06-04
 */
public class SimpleBufferConsumer<E, C> implements BufferConsumer<E> {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBufferConsumer.class);

    private final AtomicLong counter = new AtomicLong();
    private final ConsumerStrategy consumerStrategy;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Supplier<C> bufferFactory;
    private final AtomicReference<C> buffer = new AtomicReference<>();
    private final ToIntBiFunction<C, E> queueAdder;
    private final ThrowableConsumer<C, Throwable> consumer;
    private final long maxBufferCount;
    private final Consumer<E> rejectHandler;
    private final String name;
    private final ReadLock readLock;
    private final WriteLock writeLock;

    private volatile long lastConsumeTimestamp = currentTimeMillis();

    public SimpleBufferConsumer(SimpleBufferConsumerBuilder<E, C> builder) {
        this.consumerStrategy = builder.consumerStrategy;
        this.scheduledExecutorService = builder.scheduledExecutorService;
        this.bufferFactory = builder.bufferFactory;
        this.queueAdder = builder.queueAdder;
        this.consumer = builder.consumer;
        this.maxBufferCount = builder.maxBufferCount;
        this.rejectHandler = builder.rejectHandler;
        this.buffer.set(bufferFactory.get());
        this.name = builder.name;
        if (builder.disableSwitchLock) {
            readLock = null;
            writeLock = null;
        } else {
            ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
            readLock = lock.readLock();
            writeLock = lock.writeLock();
        }
        scheduledExecutorService.schedule(
                new ConsumerRunnable(scheduledExecutorService, consumerStrategy),
                DEFAULT_NEXT_TRIGGER_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(E element) {
        long currentCount = counter.get();
        if (maxBufferCount > 0 && maxBufferCount <= currentCount) {
            if (rejectHandler != null) {
                rejectHandler.accept(element);
            }
            return;
        }
        boolean locked = false;
        if (readLock != null) {
            try {
                readLock.lock();
                locked = true;
            } catch (Exception e) {
                // ignore lock failed
            }
        }
        try {
            int changeCount = queueAdder.applyAsInt(buffer.get(), element);
            if (changeCount > 0) {
                counter.addAndGet(changeCount);
            }
        } finally {
            if (locked) {
                readLock.unlock();
            }
        }
    }

    @Override
    public void doConsume() {
        synchronized (SimpleBufferConsumer.class) {
            C data;
            try {
                if (writeLock != null) {
                    writeLock.lock();
                }
                try {
                    data = buffer.getAndSet(bufferFactory.get());
                } finally {
                    counter.set(0L);
                    if (writeLock != null) {
                        writeLock.unlock();
                    }
                }
                if (data != null) {
                    consumer.accept(data);
                }
            } catch (Throwable t) {
                logger.error("BUFFER CONSUMER ERROR", t);
            }
        }
    }

    @Override
    public long getPendingChanges() {
        return counter.get();
    }

    private class ConsumerRunnable implements Runnable {

        private final ScheduledExecutorService scheduledExecutorService;
        private final ConsumerStrategy consumerStrategy;

        public ConsumerRunnable(ScheduledExecutorService scheduledExecutorService,
                ConsumerStrategy consumerStrategy) {
            this.scheduledExecutorService = scheduledExecutorService;
            this.consumerStrategy = consumerStrategy;
        }

        @Override
        public void run() {
            synchronized (SimpleBufferConsumer.class) {
                long nextConsumePeriod = 0L;
                try {
                    ConsumerStrategy.ConsumerCursor consumerCursor = consumerStrategy
                            .canConsume(lastConsumeTimestamp, counter.get());
                    if (consumerCursor.isDoConsume()) {
                        doConsume();
                        lastConsumeTimestamp = System.currentTimeMillis();
                    }
                    nextConsumePeriod = consumerCursor.getNextPeriod();
                } catch (Exception e) {
                    logger.error("BUFFER CONSUMER RUN ERROR", e);
                }
                scheduledExecutorService.schedule(this, nextConsumePeriod, TimeUnit.MILLISECONDS);
            }
        }
    }

}
