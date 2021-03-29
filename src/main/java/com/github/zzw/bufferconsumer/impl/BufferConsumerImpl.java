package com.github.zzw.bufferconsumer.impl;

import static java.lang.System.currentTimeMillis;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.zzw.bufferconsumer.BufferConsumer;
import com.github.zzw.bufferconsumer.ConsumerStrategy;
import com.github.zzw.bufferconsumer.ThrowableConsumer;

/**
 * @author zhangzhewei
 * Created on 2019-06-04
 * 批量消费简单实现
 * 可根据业务不同选择多种缓存存储器-来实现更多种类的批量消费
 */
public class BufferConsumerImpl<T, C> implements BufferConsumer<T> {

    private static final Logger logger = LoggerFactory.getLogger(BufferConsumerImpl.class);

    private final AtomicLong counter = new AtomicLong();
    private final Supplier<C> bufferFactory;//缓存器存储
    private final AtomicReference<C> buffer = new AtomicReference<>();
    //入队器
    private final ToIntBiFunction<C, T> queueAdder;
    private final ThrowableConsumer<C, Throwable> consumer;//批量消费方法
    private final long maxBufferCount;//最大缓存数量
    //拒绝策略
    private final Consumer<T> rejectHandler;
    //读写锁
    private final ReentrantLock lock;

    private volatile long lastConsumeTimestamp = currentTimeMillis();

    public BufferConsumerImpl(SimpleBufferConsumerBuilder<T, C> builder) {
        ConsumerStrategy consumerStrategy = builder.consumerStrategy;
        ScheduledExecutorService scheduledExecutorService = builder.scheduledExecutorService;
        this.bufferFactory = builder.bufferFactory;
        this.queueAdder = builder.queueAdder;
        this.consumer = builder.consumer;
        this.maxBufferCount = builder.maxBufferCount;
        this.rejectHandler = builder.rejectHandler;
        this.buffer.set(bufferFactory.get());
        if (builder.disableSwitchLock) {
            lock = null;
        } else {
             lock = new ReentrantLock();
        }
        //使用的是一个每一秒执行一次的定时任务检查是否满足消费条件
        scheduledExecutorService.schedule(
                new ConsumerRunnable(scheduledExecutorService, consumerStrategy),
                SimpleBufferConsumerBuilder.DEFAULT_NEXT_TRIGGER_PERIOD, TimeUnit.MILLISECONDS);
    }

    @Override
    public void enqueue(T element) {
        long currentCount = counter.get();
        if (maxBufferCount > 0 && maxBufferCount <= currentCount) {
            if (rejectHandler != null) {
                rejectHandler.accept(element);
            }
            return;
        }
        boolean locked = false;
        //入队的时候要加锁
        if (lock != null) {
            try {
                lock.lock();
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
                lock.unlock();
            }
        }
    }

    @Override
    public void doConsume() {
        synchronized (BufferConsumerImpl.class) {
            C data;
            try {
                if (lock != null) {
                    lock.lock();
                }
                try {
                    data = buffer.getAndSet(bufferFactory.get());
                } finally {
                    counter.set(0L);
                    if (lock != null) {
                        lock.unlock();
                    }
                }
                if (data != null) {
                    consumer.accept(data);
                }
            } catch (Throwable t) {
                logger.error("buffer consume doConsume error", t);
            }
        }
    }

    /**
     * 获取队列长度
     */
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
            synchronized (BufferConsumerImpl.class) {
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
                    //catch异常防止影响下次任务
                    logger.error("buffer consume run error", e);
                }
                scheduledExecutorService.schedule(this, nextConsumePeriod, TimeUnit.MILLISECONDS);
            }
        }
    }

}
