package com.example.pocfilterviews.repository;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.util.Collection;

import static java.util.stream.Collectors.joining;

@Repository
@RequiredArgsConstructor
public class BloomFilterRepository {

    private final JdbcTemplate jdbcTemplate;

    public boolean tryUpdate(Long passportId, Collection<Long> indexes) {
        var filterUpdateSection = indexes.stream().map(index -> "filter[" + index + "]=true").collect(joining(","));
        String sql = "update bloom_filter set " + filterUpdateSection + " where passport_id = ?";

        return jdbcTemplate.update(sql, passportId) > 0;
    }

    public void insert(Long passportId) {
        String sql = "insert into bloom_filter values (?, '{}') on conflict do nothing";

        jdbcTemplate.update(sql, passportId);
    }

    public boolean mightContain(Long passportId, Collection<Long> indexes) {
        var bloomFilterSection = indexes.stream().map(index -> "filter[" + index + "]=true").collect(joining(" and "));
        String sql = "select true from bloom_filter where passport_id = ? and " + bloomFilterSection;

        return jdbcTemplate.query(sql, this::extractExistenceCheck, passportId);
    }

    @SneakyThrows
    private boolean extractExistenceCheck(ResultSet rs) {
        return rs.next();
    }

}
