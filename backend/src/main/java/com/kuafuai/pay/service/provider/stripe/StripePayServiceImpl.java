package com.kuafuai.pay.service.provider.stripe;

import com.kuafuai.common.dynamic_config.service.DynamicConfigBusinessService;
import com.kuafuai.common.util.DateUtils;
import com.kuafuai.login.handle.GlobalAppIdFilter;
import com.kuafuai.pay.business.domain.OrderCreatRequest;
import com.kuafuai.pay.domain.PayCallbackRequest;
import com.kuafuai.pay.domain.PayLoginVo;
import com.kuafuai.pay.domain.PaymentOrderDetail;
import com.kuafuai.pay.enums.PayStatus;
import com.kuafuai.pay.service.PayService;
import com.stripe.exception.StripeException;
import com.stripe.model.Event;
import com.stripe.model.EventDataObjectDeserializer;
import com.stripe.model.StripeObject;
import com.stripe.model.checkout.Session;
import com.stripe.net.RequestOptions;
import com.stripe.net.Webhook;
import com.stripe.param.checkout.SessionCreateParams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


@Component("stripe")
@Scope("prototype")
@Service
@Slf4j
public class StripePayServiceImpl implements PayService<PaymentOrderDetail> {


    @Resource
    private DynamicConfigBusinessService dynamicConfigBusinessService;


    @Override
    public Map<String, Object> createPaymentOrder(PayLoginVo login, String orderId, BigDecimal amount, String subject, OrderCreatRequest extraParams, String database) {

        long expireTime = System.currentTimeMillis() / 1000 + 30 * 60;

        SessionCreateParams params = SessionCreateParams.builder()
                .addAllPaymentMethodType(
                        Arrays.asList(SessionCreateParams.PaymentMethodType.CARD,
                        SessionCreateParams.PaymentMethodType.LINK)
                )
                .setMode(SessionCreateParams.Mode.PAYMENT)
                .setSuccessUrl(extraParams.getCurrentUrl()+"&stripe_callback="+orderId)
                .setCancelUrl(extraParams.getCurrentUrl()+"&stripe_callback="+orderId)
                .setCustomerCreation(SessionCreateParams.CustomerCreation.IF_REQUIRED)
                .setAllowPromotionCodes(true)
                .setExpiresAt(expireTime)
                .setClientReferenceId(orderId)
                .addLineItem(
                        SessionCreateParams.LineItem.builder()
                                .setQuantity(1L)
                                .setPriceData(
                                        SessionCreateParams.LineItem.PriceData.builder()
                                                .setCurrency("usd")
                                                .setUnitAmount(amount.multiply(new BigDecimal(100)).longValue())
                                                .setProductData(
                                                        SessionCreateParams.LineItem.PriceData.ProductData.builder()
                                                                .setName(subject)
                                                                .setDescription(subject+" 商品购买")
                                                                .build()
                                                )
                                                .build()
                                )
                                .build()
                ).build();

        // 根据 appid 查询到对应的 stripe key
        Map<String, String> map = dynamicConfigBusinessService.getSystemConfig(GlobalAppIdFilter.getAppId());
        if (map.isEmpty()) {
            throw new RuntimeException("配置异常:" + GlobalAppIdFilter.getAppId());
        }

        RequestOptions requestOptions = RequestOptions.builder()
                .setApiKey(map.getOrDefault("stripe.pay.secretKey",""))
                .build();
        try {
            Map<String, Object> paramMap = new HashMap<>();
            Session session = Session.create(params, requestOptions);
            paramMap.put("sessionUrl", session.getUrl());
            paramMap.put("sessionId", session.getId());
            paramMap.put("orderNo", orderId);
            return paramMap; // 返回Checkout Session的URL，前端重定向到这个URL进行支付
        } catch (StripeException e) {
            throw new RuntimeException("Error creating checkout session:" + e.getMessage());
        }

    }


    @Override
    public String getPayId(Map<String, Object> param) {
        return (String) param.getOrDefault("sessionId","");
    }

    @Override
    public Object getPaymentParam(String sessionId, Map<String, Object> extraParams) {

        return extraParams.getOrDefault("sessionUrl","");
    }

    @Override
    public PayStatus queryPaymentStatus(String paymentOrderId) {
        return null;
    }

    @Override
    public boolean cancelPaymentOrder(String paymentOrderId) {
        return false;
    }

    @Override
    public String applyRefund(String refundOrderNo, String paymentOrderId, BigDecimal refundAmount, String reason, BigDecimal totalAmount) {
        return "";
    }

    @Override
    public PayStatus queryRefundStatus(String refundOrderId) {
        return null;
    }

    @Override
    public boolean processPaymentCallback(Object callbackData) {
        return false;
    }

    @Override
    public boolean processRefundCallback(Object callbackData) {
        return false;
    }

    @Override
    public boolean closePaymentOrder(String paymentOrderId, String orderNo) {
        return false;
    }

    @Override
    public PaymentOrderDetail getPaymentOrderDetail(String orderNo) {
        return null;
    }

    @Override
    public Object payCallbackProcessSuccess() {
        return null;
    }

    @Override
    public Object payCallbackProcessFail() {
        return null;
    }

    @Override
    public PayCallbackRequest callbackDecryption(Object requestData, Map<String, String> headers, String database) {

        String ss = headers.getOrDefault("stripe-signature", "");
        Map<String, String> map = dynamicConfigBusinessService.getSystemConfig(database);
        if (map.isEmpty()) {
            throw new RuntimeException("配置异常:" + GlobalAppIdFilter.getAppId());
        }
        try {
            Event event = Webhook.constructEvent((String) requestData, ss, map.getOrDefault("stripe.webhook.secret", ""));
            // 根据事件类型处理
            if (event.getType().equals("checkout.session.completed")) {
                return handleCheckoutSessionCompleted(event);
            } else {
                return null;
            }
        } catch (Exception e) {
            throw new RuntimeException("Webhook 签名验证失败"+ e);
        }
    }

    private PayCallbackRequest handleCheckoutSessionCompleted(Event event) {

        EventDataObjectDeserializer dataObjectDeserializer = event.getDataObjectDeserializer();
        StripeObject stripeObject = dataObjectDeserializer.getObject().orElse(null);
        PayCallbackRequest payCallbackRequest = new PayCallbackRequest();
        if (stripeObject != null) {

            Session session = (Session) stripeObject;
            payCallbackRequest.setPayStatus(getStripePaymentStatus(session.getPaymentStatus()));
            payCallbackRequest.setPaymentOrderId(session.getId());
            payCallbackRequest.setOrderNo(session.getClientReferenceId());
            payCallbackRequest.setPaymentTime(DateUtils.getLocalDatetime(session.getCreated()));
            payCallbackRequest.setPaymentAmount(new BigDecimal(session.getAmountTotal()).divide(new BigDecimal(100))); // 支付金额
        } else {
            // 反序列化失败
            throw new IllegalArgumentException("Unable to deserialize event data object");
        }

        return payCallbackRequest;

    }

    private PayStatus getStripePaymentStatus(String paymentStatus) {

        if (paymentStatus.equals("paid")) {
            return PayStatus.PAID_SUCCESS;
        }else {
            return PayStatus.PAID_FAIL;
        }
    }
}
