package com.kuafuai.pay.factoary;

import com.kuafuai.common.exception.BusinessException;
import com.kuafuai.common.util.SpringUtils;
import com.kuafuai.pay.service.PayService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PayFactory {
    public PayService getPayService(String database, String payChannel) {
        try {
            return SpringUtils.getBean(payChannel);
        } catch (Exception e) {
            log.error("getPayService", e);
            throw new BusinessException(payChannel + " not found");
        }
    }
}
