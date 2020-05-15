//package com.chryl.config;
//
//import org.springframework.context.annotation.Configuration;
//import org.springframework.security.config.annotation.web.builders.HttpSecurity;
//import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
//
///**
// * 此处是为了 测试swagger ui 出现错误时的测试配置,但是未真是下列配置是否奏效,但是清除缓存 也起作用了,就未测试下面的配置
// * Created by Chr.yl on 2020/4/22.
// * 在使用SpringBoot中配置Swagger2的时候，出现
// * <p>
// * Unable to infer base url. This is common when using dynamic servlet registration or when the API is behind an API Gateway.
// * The base url is the root of where all the swagger resources are served. For e.g.
// * if the api is available at http://example.org/api/v2/api-docs then the base url is http://example.org/api/.
// * Please enter the location manually:
// * 可能由以下几个原因造成：
// * 需要在SpringBoot的启动Application前面加上 @EnableSwagger2注解；
// * 可能是由于使用了Spring Security 影响：尝试使用以下Spring Security配置解决：
// *
// * @author Chr.yl
// */
////@Configuration
////public class SecurityConfig extends WebSecurityConfigurerAdapter {
////
////    private static final String[] AUTH_WHITELIST = {
////
////            // -- swagger ui
////            "/swagger-resources/**",
////            "/swagger-ui.html",
////            "/v2/api-docs",
////            "/webjars/**"
////    };
////
////    @Override
////    protected void configure(HttpSecurity http) throws Exception {
////        http.authorizeRequests()
////                .antMatchers(AUTH_WHITELIST).permitAll()
////                .antMatchers("/**/*").denyAll();
////    }
////}
