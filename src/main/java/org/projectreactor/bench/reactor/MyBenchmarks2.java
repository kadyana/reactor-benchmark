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

    public static final int N = 100_000;

    public static int BACKLOG = 1024 * 1024;

    @Measurement(iterations = 5, time = 1)
    @Warmup(iterations = 5, time = 1)
    @Fork(value = 3, jvmArgs = { "-Xmx1024m" })
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @State(Scope.Thread)
    public static abstract class AbstractBenchmark {

        Event<?> event;

        volatile boolean processed;

        protected Consumer<Event<?>> consumer;

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

        @Benchmark
        public void benchmark2() {
            processed = false;

            for (int i = 0; i < N; i++) {
                doBenchmark();
            }

            while (!processed) {}
        }

        protected void createCustomer() {
            this.consumer = new Consumer<Event<?>>() {

                int counter = 0;

                @Override
                public void accept(Event<?> event) {
                    counter++;
                    if(counter == N) {
                        processed = true;
                        counter = 0;
                    }
                }

            };
        }

    }

    public static class RingBufferDispatcher_Benchmark extends AbstractBenchmark {

        RingBufferDispatcher dispatcher;

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher(
                    "dispatcher",
                    BACKLOG,
                    null,
                    ProducerType.MULTI,
                    new BusySpinWaitStrategy()
            );
            createCustomer();
        }

        @Override
        protected void doTearDown() {
            dispatcher.awaitAndShutdown(5, TimeUnit.SECONDS);
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

        @Override
        protected void doSetup() {
            dispatcher = new RingBufferDispatcher2("rbd2", BACKLOG, null, ProducerType.MULTI, new BusySpinWaitStrategy());
            createCustomer();
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
            processor = RingBufferProcessor.share("processor", BACKLOG, new BusySpinWaitStrategy());
            processor.subscribe(new Subscriber<Event>() {

                int counter = 0;

                @Override
                public void onSubscribe(Subscription s) {
                    s.request(Long.MAX_VALUE);
                }

                @Override
                public void onNext(Event event) {
                    counter++;
                    if (counter == N) {
                        processed = true;
                        counter = 0;
                    }
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
