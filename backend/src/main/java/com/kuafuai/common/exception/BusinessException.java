package com.kuafuai.common.exception;


import com.kuafuai.common.domin.ErrorCode;
import com.kuafuai.common.util.I18nUtils;

/**
 * 自定义异常类
 *
 * @author lmx
 */
public class BusinessException extends RuntimeException {

    private final int code;
    private final Object[] args;

    public BusinessException(String messageKey) {
        super(I18nUtils.getOrDefault(messageKey, messageKey));
        this.code = ErrorCode.SYSTEM_ERROR.getCode();
        this.args = null;
    }

    public BusinessException(String messageKey, Object... args) {
        super(I18nUtils.getOrDefault(messageKey, messageKey, args));
        this.code = ErrorCode.SYSTEM_ERROR.getCode();
        this.args = args;
    }

    public BusinessException(int code, String messageKey) {
        super(I18nUtils.getOrDefault(messageKey, messageKey));
        this.code = code;
        this.args = null;
    }

    public BusinessException(int code, String messageKey, Object... args) {
        super(I18nUtils.getOrDefault(messageKey, messageKey, args));
        this.code = code;
        this.args = args;
    }

    public BusinessException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.code = errorCode.getCode();
        this.args = null;
    }

    public BusinessException(ErrorCode errorCode, String messageKey) {
        super(I18nUtils.getOrDefault(messageKey, messageKey));
        this.code = errorCode.getCode();
        this.args = null;
    }

    public BusinessException(ErrorCode errorCode, String messageKey, Object... args) {
        super(I18nUtils.getOrDefault(messageKey, messageKey, args));
        this.code = errorCode.getCode();
        this.args = args;
    }

    public int getCode() {
        return code;
    }

    public Object[] getArgs() {
        return args;
    }
}
