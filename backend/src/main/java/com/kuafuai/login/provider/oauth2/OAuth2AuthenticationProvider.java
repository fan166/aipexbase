package com.kuafuai.login.provider.oauth2;

import com.kuafuai.common.login.LoginUser;
import com.kuafuai.login.entity.OAuth2LoginRequest;
import com.kuafuai.login.entity.OAuth2UserInfo;
import com.kuafuai.login.provider.oauth2.server.OAuth2ProviderInterface;
import com.kuafuai.login.service.OAuth2Service;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Objects;

/**
 * OAuth2认证提供者
 */
@Slf4j
@Component
public class OAuth2AuthenticationProvider implements AuthenticationProvider {
    
    @Resource
    private OAuth2Service oauth2Service;
    

    
    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        OAuth2Authentication authToken = (OAuth2Authentication) authentication;
        OAuth2LoginRequest loginRequest = authToken.getLoginRequest();
        try {
            // 获取OAuth2提供商
            OAuth2ProviderInterface provider = oauth2Service.getProvider(loginRequest.getProvider());
            if (Objects.isNull(provider)) {
                throw new BadCredentialsException("不支持的 OAuth2 提供商: " + loginRequest.getProvider());
            }
            
            // 获取访问令牌
            String accessToken = provider.getAccessToken(loginRequest.getCode());
            
            // 获取用户信息
            OAuth2UserInfo userInfo = provider.getUserInfo(accessToken);
            
            // 查找或创建用户
            LoginUser loginUser = oauth2Service.findOrCreateUser(userInfo, loginRequest.getRelevanceTable());
            
            return new OAuth2Authentication(loginUser, accessToken, authentication.getAuthorities());
            
        } catch (Exception e) {
            log.error("OAuth2认证失败", e);
            throw new BadCredentialsException("OAuth2认证失败: " + e.getMessage());
        }
    }
    
    @Override
    public boolean supports(Class<?> authentication) {
        return OAuth2Authentication.class.isAssignableFrom(authentication);
    }
}
