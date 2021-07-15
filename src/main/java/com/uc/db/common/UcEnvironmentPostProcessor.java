package com.uc.db.common;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;

import java.util.Properties;

public class UcEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static ConfigurableEnvironment configurableEnvironment;
    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
//        this.configurableEnvironment = environment;
//        Properties properties = new Properties();
//        properties.put("test.init", "value");
//        PropertiesPropertySource source = new PropertiesPropertySource("custom", properties);
//        environment.getPropertySources().addLast(source);
    }

    public static void addProperties(String name, Properties properties){
//        PropertiesPropertySource source = new PropertiesPropertySource(name, properties);
//        configurableEnvironment.getPropertySources().addLast(source);
    }
}
