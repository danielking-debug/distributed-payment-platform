package com.daniel.distributed;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DistributedPaymentPlatformApplication {

    public static void main(String[] args) {
        SpringApplication.run(DistributedPaymentPlatformApplication.class, args);
    }
}
