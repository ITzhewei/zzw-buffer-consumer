package com.github.zzw.bufferconsumer.impl;

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

import com.github.zzw.bufferconsumer.BufferConsumer;
import com.github.zzw.bufferconsumer.ConsumerStrategy;
import com.github.zzw.bufferconsumer.ConsumerStrategy.ConsumerCursor;
import com.github.zzw.bufferconsumer.ThrowableConsumer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * @author zhangzhewei
 * Created on 2019-06-04
 * 批量消费简单构造
 */
@SuppressWarnings("unchecked")
public class SimpleBufferConsumerBuilder<T, C> {

    private static final Logger logger = LoggerFactory.getLogger(SimpleBufferConsumerBuilder.class);

    public static final long DEFAULT_NEXT_TRIGGER_PERIOD = TimeUnit.SECONDS.toMillis(1);

    //消费策略
    protected ConsumerStrategy consumerStrategy;
    //调度执行
    protected ScheduledExecutorService scheduledExecutorService;
    //队列工厂
    protected Supplier<C> bufferFactory;
    //入队器
    protected ToIntBiFunction<C, T> queueAdder;
    //消费器
    protected ThrowableConsumer<C, Throwable> consumer;
    //最大缓存数量
    protected long maxBufferCount = -1;
    //拒绝策略
    protected Consumer<T> rejectHandler;
    protected String name;
    protected boolean disableSwitchLock;

    public SimpleBufferConsumerBuilder<T, C> container(Supplier<? extends C> factory) {
        checkNotNull(factory);
        this.bufferFactory = (Supplier<C>) factory;
        return this;
    }

    public SimpleBufferConsumerBuilder<T, C> queueAdder(ToIntBiFunction<? super C, ? super T> adder) {
        checkNotNull(adder);
        this.queueAdder = (ToIntBiFunction<C, T>) adder;
        return this;
    }

    public SimpleBufferConsumerBuilder<T, C> setScheduleExecutorService(
            ScheduledExecutorService scheduleExecutorService) {
        checkNotNull(scheduleExecutorService);

        this.scheduledExecutorService = scheduleExecutorService;
        return this;
    }

    public SimpleBufferConsumerBuilder<T, C> consumerStrategy(ConsumerStrategy consumerStrategy) {
        checkNotNull(consumerStrategy);
        this.consumerStrategy = consumerStrategy;
        return this;
    }

    /**
     * 每隔多长时间执行一次
     */
    public SimpleBufferConsumerBuilder<T, C> interval(long interval, TimeUnit unit) {
        this.consumerStrategy = (lastConsumeTimestamp, changeCount) -> {
            long intervalInMs = unit.toMillis(interval);
            return ConsumerCursor
                    .cursor(changeCount > 0 && System.currentTimeMillis() - lastConsumeTimestamp >= intervalInMs,
                            intervalInMs);
        };
        return this;
    }

    /**
     * 缓存数量达到count 或者 每隔interval时间 执行一次
     */
    public SimpleBufferConsumerBuilder<T, C> interval(long count, long interval, TimeUnit unit) {
        this.consumerStrategy = (lastConsumeTimestamp, changeCount) -> {
            long intervalInMs = unit.toMillis(interval);
            return ConsumerStrategy.ConsumerCursor
                    .cursor(changeCount > count || System.currentTimeMillis() - lastConsumeTimestamp >= intervalInMs,
                            DEFAULT_NEXT_TRIGGER_PERIOD);
        };
        return this;
    }

    /**
     * 缓存数量达到count时, 执行消费
     */
    public SimpleBufferConsumerBuilder<T, C> interval(long count) {
        this.consumerStrategy =
                (lastConsumeTimestamp, changeCount) -> ConsumerStrategy.ConsumerCursor
                        .cursor(changeCount >= count, DEFAULT_NEXT_TRIGGER_PERIOD);
        return this;
    }

    public SimpleBufferConsumerBuilder<T, C> consumer(ThrowableConsumer<? extends C, Throwable> consumer) {
        checkNotNull(consumer);
        this.consumer = (ThrowableConsumer<C, Throwable>) consumer;
        return this;
    }

    public SimpleBufferConsumerBuilder<T, C> maxBufferCount(long count) {
        this.maxBufferCount = count;
        return this;
    }

    private void ensure() {
        checkNotNull(consumer);
        if (consumerStrategy == null) {
            consumerStrategy = (t, n) -> ConsumerStrategy.ConsumerCursor.empty();
        }
        if (bufferFactory == null && queueAdder == null) {
            bufferFactory = () -> (C) newSetFromMap(new ConcurrentHashMap<>());
            queueAdder = (c, e) -> ((Set<T>) c).add(e) ? 1 : 0;
        }
        if (scheduledExecutorService == null) {
            scheduledExecutorService = makeScheduleExecutor();
        }
    }

    private ScheduledExecutorService makeScheduleExecutor() {
        String threadPattern = name == null ? "buffer-consumer-thread-%d" : "buffer-consumer-thread-[" + name + "]";
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
                .setNameFormat(threadPattern)
                .setDaemon(true);
        return newSingleThreadScheduledExecutor(builder.build());
    }

    public BufferConsumer<T> build() {
        return new LazyBufferConsumer<>(() -> {
            ensure();
            SimpleBufferConsumerBuilder<T, C> builder = SimpleBufferConsumerBuilder.this;
            return new BufferConsumerImpl<>(builder);
        });
    }
}
