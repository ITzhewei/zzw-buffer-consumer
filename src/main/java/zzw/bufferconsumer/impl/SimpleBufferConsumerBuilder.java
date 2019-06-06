package zzw.bufferconsumer.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.phantomthief.util.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import zzw.bufferconsumer.BufferConsumer;
import zzw.bufferconsumer.ConsumerStrategy;

/**
 * @author zhangzhewei
 * Created on 2019-06-04
 */
public class SimpleBufferConsumerBuilder<E, C> {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBufferConsumerBuilder.class);

    public static final long DEFAULT_NEXT_TRIGGER_PERIOD = TimeUnit.SECONDS.toMillis(1);

    ConsumerStrategy consumerStrategy;
    ScheduledExecutorService scheduledExecutorService;
    Supplier<C> bufferFactory;
    ToIntBiFunction<C, E> queueAdder;
    ThrowableConsumer<C, Throwable> consumer;
    long maxBufferCount = -1;
    Consumer<E> rejectHandler;
    String name;
    boolean disableSwitchLock;

    public SimpleBufferConsumerBuilder<E, C> container(Supplier<? extends C> factory) {
        checkNotNull(factory);
        this.bufferFactory = (Supplier<C>) factory;
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C>
            queueAdder(ToIntBiFunction<? super C, ? super E> adder) {
        checkNotNull(adder);
        this.queueAdder = (ToIntBiFunction<C, E>) adder;
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C>
            setScheduleExecutorService(ScheduledExecutorService scheduleExecutorService) {
        checkNotNull(scheduleExecutorService);

        this.scheduledExecutorService = scheduleExecutorService;
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C> consumerStrategy(ConsumerStrategy consumerStrategy) {
        checkNotNull(consumerStrategy);

        this.consumerStrategy = consumerStrategy;
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C> interval(long interval, TimeUnit unit) {
        this.consumerStrategy = (lastConsumeTimestamp, changeCount) -> {
            long intervalInMs = unit.toMillis(interval);
            return ConsumerStrategy.ConsumerCursor.cursor(
                    changeCount > 0
                            && System.currentTimeMillis() - lastConsumeTimestamp >= intervalInMs,
                    intervalInMs);
        };
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C> interval(long count, long interval, TimeUnit unit) {
        this.consumerStrategy = (lastConsumeTimestamp, changeCount) -> {
            long intervalInMs = unit.toMillis(interval);
            return ConsumerStrategy.ConsumerCursor.cursor(
                    changeCount > count
                            && System.currentTimeMillis() - lastConsumeTimestamp >= intervalInMs,
                    intervalInMs);
        };
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C> interval(long count) {
        this.consumerStrategy = (lastConsumeTimestamp,
                changeCount) -> ConsumerStrategy.ConsumerCursor.cursor(changeCount >= count,
                        DEFAULT_NEXT_TRIGGER_PERIOD);
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C>
            consumer(ThrowableConsumer<? extends C, Throwable> consumer) {
        checkNotNull(consumer);
        this.consumer = (ThrowableConsumer<C, Throwable>) consumer;
        return this;
    }

    public SimpleBufferConsumerBuilder<E, C> setMaxBufferCount(long count) {
        this.maxBufferCount = count;
        return this;
    }

    //    public SimpleBufferConsumerBuilder<E, C>
    //    setExceptionHandler(BiConsumer<? super Throwable, ? super C> exceptionHandler) {
    //        checkNotNull(exceptionHandler);
    //
    //        this.ex = exceptionHandler;
    //        return this;
    //    }

    private void ensure() {
        checkNotNull(consumer);

        if (consumerStrategy == null) {
            logger.warn("no trigger strategy found. using NO-OP trigger");
            consumerStrategy = (t, n) -> ConsumerStrategy.ConsumerCursor.empty();
        }

        if (bufferFactory == null && queueAdder == null) {
            bufferFactory = () -> (C) newSetFromMap(new ConcurrentHashMap<>());
            queueAdder = (c, e) -> ((Set<E>) c).add(e) ? 1 : 0;
        }
        if (scheduledExecutorService == null) {
            scheduledExecutorService = makeScheduleExecutor();
        }
    }

    private ScheduledExecutorService makeScheduleExecutor() {
        String threadPattern = name == null ? "pool-simple-buffer-consumer-thread-%d" : "pool-simple-buffer-consumer-thread-["
                + name + "]";
        return newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()//
                .setNameFormat(threadPattern) //
                .setDaemon(true) //
                .build());
    }

    public BufferConsumer<E> build() {
        return new LazyBufferConsumer<>(() -> {
            ensure();
            SimpleBufferConsumerBuilder<E, C> builder = SimpleBufferConsumerBuilder.this;
            return new SimpleBufferConsumer<>(builder);
        });
    }
}
