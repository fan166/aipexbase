package com.kuafuai.login.provider.oauth2;

import com.kuafuai.login.entity.OAuth2LoginRequest;
import lombok.Data;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;

import java.util.Collection;

/**
 * OAuth2认证令牌
 */
@Data
public class OAuth2Authentication extends AbstractAuthenticationToken {
    
    private OAuth2LoginRequest loginRequest;
    private Object principal;
    private Object credentials;
    
    public OAuth2Authentication(OAuth2LoginRequest loginRequest) {
        super(null);
        this.loginRequest = loginRequest;
        this.setAuthenticated(false);
    }
    
    public OAuth2Authentication(Object principal, Object credentials, Collection<? extends GrantedAuthority> authorities) {
        super(authorities);
        this.principal = principal;
        this.credentials = credentials;
        this.setAuthenticated(true);
    }
    
    @Override
    public Object getCredentials() {
        return credentials;
    }
    
    @Override
    public Object getPrincipal() {
        return principal;
    }
}
