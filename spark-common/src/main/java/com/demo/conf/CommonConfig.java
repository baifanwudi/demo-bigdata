package com.demo.conf;

import com.demo.common.ConfigUtil;

public class CommonConfig {

    public static final String DEPLOY_TYPE= ConfigUtil.getPros("deploy.type");

    public static final String DEFAULT_DEPLOY_TYPE="local";

    public static Boolean isLocalEnvConfig(){
        return DEPLOY_TYPE.equals(CommonConfig.DEFAULT_DEPLOY_TYPE);
    }
}
