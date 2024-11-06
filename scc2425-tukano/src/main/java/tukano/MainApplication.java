package tukano;

import jakarta.ws.rs.core.Application;
import tukano.impl.Token;
import tukano.impl.rest.RestBlobsResource;
import tukano.impl.rest.RestShortsResource;
import tukano.impl.rest.RestUsersResource;
import tukano.impl.rest.utils.CustomLoggingFilter;
import tukano.impl.rest.utils.GenericExceptionMapper;


import java.util.HashSet;
import java.util.Set;

public class MainApplication extends Application {
    private Set<Object> singletons = new HashSet<>();

    private Set<Class<?>> resources = new HashSet<>();



    public MainApplication () {
        resources.add(RestBlobsResource.class);
        resources.add(RestShortsResource.class);
        resources.add(RestUsersResource.class);

          singletons.add(new CustomLoggingFilter());
        singletons.add(new GenericExceptionMapper());

        Token.setSecret("59158");
    }

    @Override
    public Set<Class<?>> getClasses() {
        return resources;
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}
