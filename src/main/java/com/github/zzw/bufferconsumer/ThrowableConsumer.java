package com.github.zzw.bufferconsumer;

/**
 * @author zhangzhewei
 */
public interface ThrowableConsumer<T, E extends Throwable> {

    void accept(T t) throws E;

}
