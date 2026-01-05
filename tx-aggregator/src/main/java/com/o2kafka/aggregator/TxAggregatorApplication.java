package com.o2kafka.aggregator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class TxAggregatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(TxAggregatorApplication.class, args);
    }
}
