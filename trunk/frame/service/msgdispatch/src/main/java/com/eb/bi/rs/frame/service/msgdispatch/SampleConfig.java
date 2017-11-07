package com.eb.bi.rs.frame.service.msgdispatch;

import java.util.HashMap;
import java.util.Map;

import com.google.inject.Scopes;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

/**
 * Example Guice Server configuration. Creates an Injector, and binds it to
 * whatever Modules we want. In this case, we use an anonymous Module, but other
 * modules are welcome as well.
 */
public class SampleConfig extends GuiceServletContextListener {
    @Override
    protected Injector getInjector() {
        return Guice.createInjector(new ServletModule() {
            @Override
            protected void configureServlets() {
                /* bind the REST resources */

                bind(SampleResource.class);

                /* bind jackson converters for JAXB/JSON serialization */
//                bind(MessageBodyReader.class).to(JacksonJsonProvider.class);
//                bind(MessageBodyWriter.class).to(JacksonJsonProvider.class);
                bind(JacksonJsonProvider.class).in(Scopes.SINGLETON);
                Map<String, String> initParams = new HashMap<String, String>();
                initParams.put("com.sun.jersey.config.feature.Trace",
                        "true");
                serve("*").with(
                        GuiceContainer.class,
                        initParams);
            }
        });
    }
}
