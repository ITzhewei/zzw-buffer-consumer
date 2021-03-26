package com.github.zzw;

import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.zzw.bufferconsumer.BufferConsumer;
import com.github.zzw.model.Person;
import com.google.common.collect.Lists;


/**
 * @author zhangzhewei
 * Created on 2019-06-06
 */
public class BufferConsumerTest {

    private static final Logger logger = LoggerFactory.getLogger(BufferConsumerTest.class);

    private BufferConsumer<Person> bufferConsumer = BufferConsumer.<Person, List<Person>> simpleBuilder() //
            .container(Lists::newArrayList)
            .queueAdder(this::add)
//            .interval(5, 5, TimeUnit.SECONDS) //时间间隔消费
            .interval(10) //按照数量间隔消费
//            .interval(10, 5, TimeUnit.SECONDS) //时间和数量间隔一起进行
            .maxBufferCount(1000) //最大缓存数量
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
        for (long i = 0; i < 30; i++) {
            bufferConsumer.enqueue(new Person(i, i + ""));
            Thread.sleep(1000L);
        }

    }

}
