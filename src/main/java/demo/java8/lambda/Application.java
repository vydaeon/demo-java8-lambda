package demo.java8.lambda;

import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;

@ApplicationPath("/")
public class Application extends ResourceConfig {

    public static void main(String[] args) throws Exception {
        URI baseUri = UriBuilder.fromUri("http://localhost/").port(9998).build();
        ResourceConfig config = new Application();
        final Server server = JettyHttpContainerFactory.createServer(baseUri, config);
        server.start();

        Thread shutdownHook = new Thread(() -> stop(server));
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        shutdownHook.join();
    }

    public Application() {
        packages(Application.class.getPackage().getName());
    }

    private static void stop(Server server) {
        try {
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
