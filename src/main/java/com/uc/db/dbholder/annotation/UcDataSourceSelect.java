package com.uc.db.dbholder.annotation;

import java.lang.annotation.*;

/**
 * Created by wangxiaobo on 2018/1/19.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD,ElementType.TYPE})
@Inherited
public @interface UcDataSourceSelect {
    String name() default "";
}
