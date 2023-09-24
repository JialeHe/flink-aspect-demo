package org.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@Aspect
public class FlinkAspect {
    private static final Logger logger = LoggerFactory.getLogger(FlinkAspect.class);

    @Before("execution(* org.apache.flink.table.api.TableEnvironment.executeSql(..))")
    public void beforeParserSql(JoinPoint joinPoint) {
        logger.info("beforeParserSql: {}", joinPoint.getArgs()[0]);
    }
}
