package com.platfrom.regional;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class RegionalServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(RegionalServiceApplication.class, args);
    }
}
