package com.uc.db.common;

import com.google.common.base.CaseFormat;
import com.uc.db.config.UcDataSourceBaseConfig;
import com.uc.db.config.UcPageHelperConfig;
import com.uc.db.dbholder.UcDataSourceContextHolder;
import com.uc.db.dbholder.aop.UcDataSourceAspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
public class UcBeanDefinitionRegistryPostProcessor implements BeanDefinitionRegistryPostProcessor, EnvironmentAware {

    private static final Logger logger = LoggerFactory.getLogger(UcBeanDefinitionRegistryPostProcessor.class);
    private static Environment environment;

    @Override
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

        String configFile = UcDataSourceImport.getDbConfigFilePathName();
        configFile = getPropertiesValue(configFile);
        registerBean(configFile,registry, UcDataSourceBaseConfig.class);
        if (UcDataSourceImport.dbMultiDataSource == false){
            Class [] arrayClazz = {UcPageHelperConfig.class, UcDataSourceContextHolder.class, UcDataSourceAspect.class};
            removeBean(arrayClazz,registry);
        }
    }

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
        System.out.print("b");
    }

    private void registerBean(final String configFile, final BeanDefinitionRegistry registry,Class clazz) {
        final BeanDefinitionBuilder beanDefinitionBuilder = BeanDefinitionBuilder.genericBeanDefinition(clazz);
        beanDefinitionBuilder.addConstructorArgValue(configFile);
        beanDefinitionBuilder.setInitMethodName("init");
        final BeanDefinition beanDefinition = beanDefinitionBuilder.getBeanDefinition();
        beanDefinition.setScope("singleton");       //设置scope
        beanDefinition.setLazyInit(false);          //设置是否懒加载
        beanDefinition.setAutowireCandidate(true);  //设置是否可以被其他对象自动注入
        String beanName = clazz.getSimpleName();
        beanName = getCamelName(beanName);
        registry.registerBeanDefinition(beanName, beanDefinition);
    }

    private void removeBean(Class [] arrayClazz,final BeanDefinitionRegistry registry){
        for(Class clazz : arrayClazz){
            String beanName = getCamelName(clazz.getSimpleName());
            if (registry.containsBeanDefinition(beanName) == true){
                registry.removeBeanDefinition(beanName);
            }
        }
    }

    public static String getCamelName(String className){
        String beanName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, className);
        beanName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,beanName);
        return beanName;
    }

    public static String getPropertiesValue(String value){
        return environment.resolvePlaceholders(value);
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
