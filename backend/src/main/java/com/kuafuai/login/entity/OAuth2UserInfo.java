package com.kuafuai.login.entity;

import lombok.Data;

/**
 * OAuth2用户信息实体
 * 用于封装从OAuth2提供商获取的用户信息
 */
@Data
public class OAuth2UserInfo {
    
    /**
     * 用户在OAuth2提供商系统中的唯一ID
     */
    private String id;
    
    /**
     * 用户名
     * 通常是邮箱地址或用户名
     */
    private String username;
    
    /**
     * 用户邮箱地址
     */
    private String email;
    
    /**
     * 用户头像URL
     */
    private String avatar;
    
    /**
     * 用户昵称或显示名称
     */
    private String nickname;
    
    /**
     * 用户手机号码
     * 部分OAuth2提供商可能提供此信息
     */
    private String phone;
    
    /**
     * 用户性别
     * 例如：male、female等
     */
    private String gender;
    
    /**
     * 用户地区/语言设置
     * 例如：zh-CN、en-US等
     */
    private String locale;
    
    /**
     * 原始用户信息
     * 存储从OAuth2提供商返回的完整用户信息JSON对象
     */
    private Object rawUserInfo;
}
