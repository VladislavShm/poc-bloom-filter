package com.example.pocfilterviews.service;

import com.example.pocfilterviews.repository.BloomFilterRepository;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class BloomFilterService {

    private final BloomFilterRepository bloomFilterRepository;

    public void add(long passportId, Collection<Long> videoIds) {
        var indexes = videoIds.stream().map(this::calculateBloomIndexes).flatMap(Collection::stream).collect(Collectors.toSet());
        update(passportId, indexes);
    }

    public void add(long passportId, long videoId) {
        var indexes = calculateBloomIndexes(videoId);
        update(passportId, indexes);
    }

    public boolean mightContain(Long passportId, Long videoId) {
        var indexes = calculateBloomIndexes(videoId);
        return bloomFilterRepository.mightContain(passportId, indexes);
    }

    private List<Long> calculateBloomIndexes(long value) {
        final int numHashFunctions = 10;
        final long bitSize = 100_000;

        List<Long> result = new ArrayList<>();
        long hash64 = Hashing.murmur3_128().hashObject(value, Funnels.longFunnel()).asLong();
        long hash1 = hash64;
        long hash2 = (hash64 >>> 32);

        for (int i = 1; i <= numHashFunctions; ++i) {
            long combinedHash = hash1 + i * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }

            result.add(combinedHash % bitSize);
        }

        return result;
    }

    private void update(Long passportId, Collection<Long> indexes) {
        try {
            if (bloomFilterRepository.tryUpdate(passportId, indexes)) {
                return;
            }

            try {
                bloomFilterRepository.insert(passportId);
            } catch (DuplicateKeyException e) {

            }

            if (bloomFilterRepository.tryUpdate(passportId, indexes)) {
                return;
            }

            log.warn("Can not update a bloom filter for: {}", passportId);
        } catch (Exception e) {
            log.error("Error while updating a bloom filter: ", e);
            throw e;
        }
    }

}
