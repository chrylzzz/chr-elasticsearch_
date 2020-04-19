package com.chryl.config;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Configuration;

/**
 * MyBatis配置类
 */
@Configuration
@MapperScan({"com.chryl.mapper", "com.chryl.dao"})
public class MyBatisConfig {
}
