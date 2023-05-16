package ru.michaelshell.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
class FluxTest {

    @Test
    void fluxSubscriber() {
        Flux<Integer> flux = Flux.range(2, 5).log();

        StepVerifier.create(flux)
                .expectNext(2, 3, 4, 5, 6)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberFromList() {
        Flux<Integer> flux = Flux.fromIterable(List.of(1, 2, 3));

        flux.subscribe(i -> log.info("Number {}", i));

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberNumbersError() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log()
                .handle((i, sink) -> {
                    if (i == 3) {
                        sink.error(new RuntimeException("Ex thrown"));
                        return;
                    }
                    sink.next(i);
                });

        flux.subscribe(i -> log.info("Number {}", i), Throwable::printStackTrace,
                () -> log.info("DONE!"),
                subscription -> subscription.request(2));

        log.info("---------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2)
                .expectError(RuntimeException.class)
                .verify();

    }

    @Test
    void fluxSubscriberNumberBackpressure() {
        Flux<Integer> flux = Flux.range(1, 10).log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });


        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberBackpressureRateLimit() {
        Flux<Integer> flux = Flux.range(1, 10)
                .log()
                .limitRate(2); //same result as above, should be after log() to work

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(200))
                        .take(10);

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(5000);
    }

    @Test
    void fluxSubscriberIntervalTwo() throws InterruptedException {

        StepVerifier.withVirtualTime(this::getInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofHours(24))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> getInterval() {
        return Flux.interval(Duration.ofDays(1L))
                .log();
    }

    @Test
    void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> flux = Flux.range(1, 10)
//                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        flux.connect();

//        log.info("Thread sleeps 500ms");
//        Thread.sleep(500);
//
//        flux.subscribe(i -> log.info("Sub1 number {}", i));
//
//        log.info("Thread sleeps 300ms");
//        Thread.sleep(300);
//
//        flux.subscribe(i -> log.info("Sub2 number {}", i));

        StepVerifier.create(flux)
                .then(flux::connect)
                .thenConsumeWhile(i -> i < 4)
                .expectNext(4,5,6,7,8,9,10)
                .verifyComplete();
    }


}
