package com.kuafuai.config;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ReloadableResourceBundleMessageSource;
import org.springframework.web.servlet.LocaleResolver;
import org.springframework.web.servlet.i18n.AcceptHeaderLocaleResolver;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Locale;

@Configuration
@Slf4j
public class I18nConfig {

    @Bean
    public LocaleResolver localeResolver() {
        return new HybridLocaleResolver();
    }

    @Bean
    public MessageSource messageSource() {
        ReloadableResourceBundleMessageSource messageSource = new ReloadableResourceBundleMessageSource();
        messageSource.setBasename("classpath:messages");
        messageSource.setDefaultEncoding("UTF-8");
        messageSource.setCacheSeconds(3600);
        return messageSource;
    }

    public static class HybridLocaleResolver extends AcceptHeaderLocaleResolver {

        @Override
        public void setLocale(HttpServletRequest request, HttpServletResponse response, Locale locale) {
            log.info("=================1");
        }

        @NotNull
        @Override
        public Locale resolveLocale(HttpServletRequest request) {
            // 1. 优先 URL 参数
            String langParam = request.getParameter("lang");
            if (langParam != null && !langParam.isEmpty()) {
                return parseLocale(langParam);
            }

            // 2. 其次根据请求头
            Locale locale = request.getLocale();
            if (locale != null) {
                return locale;
            }

            // 3. 默认语言
            return Locale.SIMPLIFIED_CHINESE;
        }

        private Locale parseLocale(String lang) {

            if (lang.equalsIgnoreCase("en")) {
                return new Locale("en", "US");
            } else if (lang.equalsIgnoreCase("zh")) {
                return new Locale("zh", "CN");
            }

            if (lang.contains("_")) {
                String[] parts = lang.split("_");
                return new Locale(parts[0], parts[1]);
            } else if (lang.contains("-")) {
                String[] parts = lang.split("-");
                return new Locale(parts[0], parts[1]);
            } else {
                return new Locale(lang);
            }
        }
    }
}
