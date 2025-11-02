package com.kuafuai.manage.service;

import com.kuafuai.common.login.LoginUser;
import com.kuafuai.common.login.SecurityUtils;
import com.kuafuai.manage.entity.vo.ManageLoginVo;
import com.kuafuai.system.entity.Users;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Component;

import java.util.Objects;


@Component
@Slf4j
public class ManageUserNameProvider implements AuthenticationProvider {

    @Autowired
    private ManageBusinessService manageBusinessService;

    @Override
    public Authentication authenticate(Authentication authentication) throws AuthenticationException {
        ManageLoginVo loginVo = (ManageLoginVo) authentication.getPrincipal();

        Users current = manageBusinessService.getByEmail(loginVo.getEmail());
        if (current == null) {
            throw new UsernameNotFoundException("");
        }

        if (Objects.isNull(authentication.getCredentials())) {
            throw new BadCredentialsException("");
        }

        String passwd = authentication.getCredentials().toString();

        if (!SecurityUtils.matchesPassword(passwd, current.getPassword())) {
            throw new BadCredentialsException("");
        }

        LoginUser loginUser = new LoginUser(current.getId());

        return new ManageUserNameAuthentication(loginUser, "", authentication.getAuthorities());
    }

    @Override
    public boolean supports(Class<?> authentication) {
        return authentication.equals(ManageUserNameAuthentication.class);
    }
}
