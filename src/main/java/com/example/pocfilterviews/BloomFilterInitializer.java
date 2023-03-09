package com.example.pocfilterviews;

import com.example.pocfilterviews.service.BloomFilterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Slf4j
@Component
@RequiredArgsConstructor
public class BloomFilterInitializer implements ApplicationListener<ApplicationStartedEvent> {

    private final BloomFilterService bloomFilterService;
    private final AtomicInteger      counter = new AtomicInteger(0);
    private final Long               current = System.nanoTime();

    @Override
    public void onApplicationEvent(ApplicationStartedEvent event) {
        log.info("Start initialization");
        var executorService = Executors.newFixedThreadPool(4);

        for (int i = 0; i < 10_000; i++) {
            executorService.submit(this::func2);
        }
    }

    private void func() {
//        var passportId = 1;
        var passportId = new Random().nextLong(10);
        var current = counter.incrementAndGet();
        if (current % 1000 == 0) {
            log.info("Inserting {}", current);
        }
        var videoId = new Random().nextLong(10_000_000);
        bloomFilterService.add(passportId, videoId);
    }

    private void func2() {
        var passportId = new Random().nextLong(10);
        var videos = new ArrayList<Long>();
        for (int i = 0; i < 5; i++) {
            var current = counter.incrementAndGet();
            if (current % 1000 == 0) {
                log.info("Inserting {}", current);
            }
            var videoId = new Random().nextLong(10_000_000);
            videos.add(videoId);
        }

        log.info("For {} adding {}", passportId, videos);
        bloomFilterService.add(passportId, videos);
    }

}
