package com.salescode.cdc.config;

import com.salescode.cdc.listener.GlobalEntityListener;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.hibernate.internal.SessionFactoryImpl;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.persistence.EntityManagerFactory;

@Configuration
@ConditionalOnProperty(name = "cdc-pipeline.enabled", havingValue = "true", matchIfMissing = false)
public class CDCProducerAutoConfiguration {

    /**
     * Create the listener bean
     */
    @Bean
    public GlobalEntityListener globalEntityListener() {
        System.out.println("✅ CDC Pipeline SDK - GlobalEntityListener created");
        return new GlobalEntityListener();
    }

    /**
     * Register the listener with Hibernate
     */
    @Bean
    public CDCListenerRegistrar listenerRegistrar(
            EntityManagerFactory entityManagerFactory,
            GlobalEntityListener listener) {

        System.out.println("✅ CDC Pipeline SDK - Registering global listener with Hibernate");

        // Get Hibernate's SessionFactory
        SessionFactoryImpl sessionFactory =
                entityManagerFactory.unwrap(SessionFactoryImpl.class);

        // Get event registry
        EventListenerRegistry registry = sessionFactory
                .getServiceRegistry()
                .getService(EventListenerRegistry.class);

        // Register our listener for all events
        registry.appendListeners(EventType.POST_INSERT, listener);
        registry.appendListeners(EventType.POST_UPDATE, listener);
        registry.appendListeners(EventType.POST_DELETE, listener);

        System.out.println("✅ CDC Pipeline SDK - Global listener registered successfully!");

        return new CDCListenerRegistrar();
    }

    /**
     * Dummy class just to hold the bean
     */
    public static class CDCListenerRegistrar {
        // Empty - just marks that registration happened
    }
}