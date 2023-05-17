package ru.michaelshell.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
class OperatorsTest {

    @Test
    void subscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void publishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void multipleSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void multiplePublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void publishAndSubscribeOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void subscribeAndPublishOnSimple() {
        Flux<Integer> flux = Flux.range(1, 5)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map1 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map2 Number {} from thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    void subscribeOnIO() {
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                .subscribeOn(Schedulers.boundedElastic());

        list.subscribe(l -> log.info("{}", l));

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(strings -> {
                    Assertions.assertFalse(strings.isEmpty());
                    log.info("size {}", strings.size());
                    return true;
                })
                .verifyComplete();
    }

    @Test
    void switchIfEmpty() {
        Flux<Object> flux = Flux.empty()
                .switchIfEmpty(Flux.just("not empty"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("not empty")
                .verifyComplete();
    }

    @Test
    void defer() throws InterruptedException {
        Mono<Long> just = Mono.just(System.currentTimeMillis());
        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100L);
        defer.subscribe(l -> log.info("time {}", l));
        Thread.sleep(100L);
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);
    }

    @Test
    void concatOperator() {
        Flux<Integer> flux1 = Flux.just(1, 2);
        Flux<Integer> flux2 = Flux.just(3, 4);

//        Flux<Integer> concat = Flux.concat(flux1, flux2);
        Flux<Integer> concat = flux1.concatWith(flux2);


        StepVerifier.create(concat)
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    void combineLatest() {
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combinedLatest = Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase());

        StepVerifier.create(combinedLatest)
                .expectNext("BC", "BD")
                .verifyComplete();
    }


}
