package com.kuafuai.login.entity;

import lombok.Data;

/**
 * 登录请求实体
 * 用于封装各种登录方式所需的参数
 */
@Data
public class LoginVo {
    
    /**
     * 小程序授权码
     * 用于微信小程序登录
     */
    private String code;

    /**
     * 用户名或手机号
     * 用于用户名/密码登录或手机号验证码登录
     */
    private String phone;

    /**
     * 密码
     * 用于用户名/密码登录
     */
    private String password;

    /**
     * 状态参数
     * 用于OAuth2登录的安全验证
     */
    private String state;

    /**
     * 关联表
     * 用户数据存储的表名，用于多租户场景
     */
    private String relevanceTable;

    /**
     * 微信OpenID
     * 用于微信相关登录
     */
    private String openId;
}
