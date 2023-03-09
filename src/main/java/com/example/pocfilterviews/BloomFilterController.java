package com.example.pocfilterviews;

import com.example.pocfilterviews.service.BloomFilterService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class BloomFilterController {
    private final BloomFilterService bloomFilterService;

    @GetMapping("/might-contain")
    public boolean isExists(@RequestParam("passportId") Long passportId, @RequestParam("videoId") Long videoId) {
        return bloomFilterService.mightContain(passportId, videoId);
    }
}
