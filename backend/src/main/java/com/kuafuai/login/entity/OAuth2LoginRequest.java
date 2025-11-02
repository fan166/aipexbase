package com.kuafuai.login.entity;

import lombok.Data;

/**
 * OAuth2登录请求实体
 * 用于封装OAuth2登录过程中需要的所有参数
 */
@Data
public class OAuth2LoginRequest {
    
    /**
     * OAuth2提供商类型
     * 例如：google、github、facebook等
     */
    private String provider;
    
    /**
     * 授权码
     * 由OAuth2提供商在用户授权后返回的授权码
     */
    private String code;
    
    /**
     * 状态参数
     * 用于防止CSRF攻击的安全参数，必须与请求授权URL时使用的state保持一致
     */
    private String state;
    
    /**
     * 关联表
     * 用户数据存储的表名，用于多租户场景
     */
    private String relevanceTable;
}
