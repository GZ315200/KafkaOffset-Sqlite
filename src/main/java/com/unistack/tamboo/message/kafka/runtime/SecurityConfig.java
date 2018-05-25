package com.unistack.tamboo.message.kafka.runtime;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;

import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/15
 */
public class SecurityConfig extends AbstractConfig {

    private static final ConfigDef CONFIG = new ConfigDef()
            .define(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    BrokerSecurityConfigs.SSL_CLIENT_AUTH_DOC)
            .define(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, ConfigDef.Type.LIST,
                    BrokerSecurityConfigs.DEFAULT_SASL_ENABLED_MECHANISMS,
                    ConfigDef.Importance.MEDIUM, BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_DOC)
            .define(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG, ConfigDef.Type.CLASS,
                    null, ConfigDef.Importance.MEDIUM, BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_DOC)
            .withClientSslSupport()
            .withClientSaslSupport();



    public SecurityConfig(Map<?, ?> originals) {
        super(CONFIG, originals, false);
    }
}
