package com.kuafuai.login.entity;

import lombok.Data;

/**
 * OAuth2提供商配置实体
 * 用于存储和管理OAuth2提供商的配置信息
 */
@Data
public class OAuth2ProviderConfig {
    
    /**
     * 提供商名称
     * 例如：google、github、facebook等
     */
    private String provider;
    
    /**
     * OAuth2客户端ID
     * 在OAuth2提供商处注册应用时获得的客户端标识
     */
    private String clientId;
    
    /**
     * OAuth2客户端密钥
     * 在OAuth2提供商处注册应用时获得的客户端密钥，用于安全验证
     */
    private String clientSecret;
    
    /**
     * 授权URL
     * OAuth2提供商的授权端点URL
     * 例如：https://accounts.google.com/o/oauth2/v2/auth
     */
    private String authorizationUri;
    
    /**
     * 令牌URL
     * OAuth2提供商的令牌端点URL
     * 用于交换授权码获取访问令牌
     * 例如：https://oauth2.googleapis.com/token
     */
    private String tokenUri;
    
    /**
     * 用户信息URL
     * OAuth2提供商的用户信息端点URL
     * 用于获取用户详细信息
     * 例如：https://www.googleapis.com/oauth2/v2/userinfo
     */
    private String userInfoUri;
    
    /**
     * 授权作用域
     * 定义应用需要访问的用户信息范围
     * 例如：openid profile email
     */
    private String scope;
    
    /**
     * 是否启用该提供商
     * true：启用，false：禁用
     */
    private Boolean enabled;
    
    /**
     * 应用ID
     * 关联的应用标识，用于多应用场景
     */
    private String appId;
}
