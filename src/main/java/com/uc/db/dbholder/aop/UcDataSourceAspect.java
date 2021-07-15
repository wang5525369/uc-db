package com.uc.db.dbholder.aop;

import com.uc.db.dbholder.UcDataSourceContextHolder;
import com.uc.db.dbholder.annotation.UcDataSourceSelect;
import com.uc.db.annotation.UcEnableDataSource;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * Created by wangxiaobo on 2018/1/19.
 */
//@Conditional(UcMultiDatasourceCondition.class)
@ConditionalOnBean(annotation = {UcEnableDataSource.class})
@Aspect
@Order(-1)
@Component
public class UcDataSourceAspect {
    public static final Logger logger = LoggerFactory.getLogger(UcDataSourceAspect.class);
    @Pointcut("@annotation(com.uc.db.dbholder.annotation.UcDataSourceSelect) || @within(com.uc.db.dbholder.annotation.UcDataSourceSelect)")
    public void changeDataSource(){
        logger.debug("切换数据源");
    };

    @Around("@annotation(ucDataSourceSelect) || @within(ucDataSourceSelect)")
    public Object proceed(ProceedingJoinPoint proceedingJoinPoint, UcDataSourceSelect ucDataSourceSelect) throws Throwable {
        try {
            if (ucDataSourceSelect == null) {
                Signature signature = proceedingJoinPoint.getSignature();
                if (signature instanceof MethodSignature) {
                    Method method = ((MethodSignature) signature).getMethod();
                    ucDataSourceSelect = (UcDataSourceSelect) method.getAnnotation(UcDataSourceSelect.class);
                }
            }

            if (ucDataSourceSelect == null){
                ucDataSourceSelect = proceedingJoinPoint.getTarget().getClass().getAnnotation(UcDataSourceSelect.class);
            }

            String dataSourceName = "";
            if(ucDataSourceSelect != null){
                dataSourceName = ucDataSourceSelect.name();
            }
            // 切换数据源
            UcDataSourceContextHolder.setDataSourceName(dataSourceName);
            Object result = proceedingJoinPoint.proceed();
            UcDataSourceContextHolder.clearDataSourceName();
            return result;
        } finally {
            UcDataSourceContextHolder.clearDataSourceName();
        }
    }
}
