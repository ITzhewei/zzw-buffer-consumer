package com.github.zzw;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.zzw.bufferconsumer.BufferConsumer;
import com.google.common.collect.Lists;


/**
 * @author zhangzhewei
 * Created on 2019-06-06
 */
public class BufferConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(BufferConsumerTest.class);

    private BufferConsumer<Person> bufferConsumer = BufferConsumer.<Person, List<Person>> simpleBuilder() //
            .container(Lists::newArrayList) //初始化容器
            .queueAdder(this::add) //添加函数
            //            .interval(3) //消费间隔
            .interval(3, TimeUnit.SECONDS).interval(2).setMaxBufferCount(1000) //最大缓存数量
            .consumer(this::doConsumer) //消费函数
            .build();

    /**
     * 入队方法
     */
    private int add(List<Person> list, Person person) {
        list.add(person);
        return 1; // 表示这次操作实际的改动数
    }

    /**
     * 消费方法
     */
    private void doConsumer(List<Person> list) {
        System.out.println("size:-------" + list.size());
        list.forEach(person -> {
            System.out.println("id:" + person.getId() + ",name:" + person.getName());
        });
    }

    @Test
    public void testConsumer() throws InterruptedException {
        for (long i = 0; i < 10; i++) {
            bufferConsumer.enqueue(new Person(i, i + ""));
            Thread.sleep(1000L);
        }
    }

}
