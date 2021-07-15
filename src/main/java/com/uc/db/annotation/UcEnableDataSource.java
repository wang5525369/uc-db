package com.uc.db.annotation;


import com.uc.db.common.UcDataSourceImport;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * Created by wangxiaobo on 2018/7/26.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Documented
@Import({UcDataSourceImport.class})
public @interface UcEnableDataSource {
    boolean isMultiDataSource() default false;
    String configFile() default "";
    String mapperPath() default "classpath*:/mapper/**/*.xml";
    boolean isShowWeb() default false;
}
