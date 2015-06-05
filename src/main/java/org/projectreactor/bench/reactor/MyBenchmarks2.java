package org.projectreactor.bench.reactor;

import org.openjdk.jmh.annotations.*;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.bus.Event;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.dispatch.RingBufferDispatcher2;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BusySpinWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.TimeUnit;

/**
 * Created by anato_000 on 6/5/2015.
 */
public class MyBenchmarks2 {

    @Measurement(iterations = 5, time = 1)
    @Warmup(iterations = 5, time = 1)
    @Fork(value = 3, jvmArgs = { "-Xmx1024m" })
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @State(Scope.Thread)
    public static abstract class AbstractBenchmark {

        Event<?> event;

        @Setup
        public void setup() {
            event = Event.wrap("Hello World!");
            doSetup();
        }

        protected abstract void doSetup();

        @TearDown
        public void tearDown() {
            doTearDown();
        }

        protected abstract void doTearDown();

        @Benchmark
        public void benchmark() {
            doBenchmark();
        }

        protected abstract void doBenchmark();

    }

    public static class RingBufferDispatcher_Benchmark extends AbstractBenchmark {

        RingBufferDispatcher dispatcher;

        Consumer<Event<?>> consumer;

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher(
                    "dispatcher",
                    MyParams.BACKLOG,
                    null,
                    ProducerType.MULTI,
                    new BusySpinWaitStrategy()
            );
            consumer = new Consumer<Event<?>>() {

                @Override
                public void accept(Event<?> event) {
                }

            };
        }

        @Override
        protected void doTearDown() {
            System.out.println("doTearDown");
            dispatcher.awaitAndShutdown(10, TimeUnit.SECONDS);
            System.out.println("after doTearDown");
        }

        @Override
        protected void doBenchmark() {
            dispatcher.dispatch(
                    event,
                    consumer,
                    null
            );
        }
    }

    public static class RingBufferDispatcher2_Benchmark extends AbstractBenchmark {

        private RingBufferDispatcher2 dispatcher;

        private Consumer<Event<?>> consumer;

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher2("rbd2", MyParams.BACKLOG, null, ProducerType.MULTI, new BusySpinWaitStrategy());

            consumer = new Consumer<Event<?>>() {

                @Override
                public void accept(Event<?> event) {
                }

            };
        }

        @Override
        protected void doTearDown() {
            dispatcher.shutdown();
        }

        @Override
        protected void doBenchmark() {
            dispatcher.dispatch(
                    event,
                    consumer,
                    null
            );
        }
    }

    public static class RingBufferProcessor_Benchmark extends AbstractBenchmark {

        private RingBufferProcessor<Event<?>> processor;

        @Override
        protected void doSetup() {
            processor = RingBufferProcessor.share("processor", MyParams.BACKLOG, new BusySpinWaitStrategy());
            processor.subscribe(new Subscriber<Event>() {

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Event event) {
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onComplete() {
                }

            });
        }

        @Override
        protected void doTearDown() {
            processor.shutdown();
        }

        @Override
        protected void doBenchmark() {
            processor.onNext(event);
        }
    }


}
