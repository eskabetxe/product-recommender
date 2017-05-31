package pro.boto.recommender.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages="pro.boto.recommender.engine")
public class EngineMain {
    public static void main(String[] args) {
        SpringApplication.run(EngineMain.class, args);
    }
}
