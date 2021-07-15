package com.uc.es.config;

import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

/**
 * @description:
 * @author: guohongbao
 * @Date: 2019/3/6 16:06
 */
@Configuration
public class UcESConfig {
    @PostConstruct
    void init() {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }
}