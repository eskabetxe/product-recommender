package pro.boto.recommender.configuration;

import org.cfg4j.provider.ConfigurationProvider;
import org.cfg4j.provider.ConfigurationProviderBuilder;
import org.cfg4j.source.ConfigurationSource;
import org.cfg4j.source.classpath.ClasspathConfigurationSource;
import org.cfg4j.source.context.filesprovider.ConfigFilesProvider;
import org.cfg4j.source.git.GitConfigurationSourceBuilder;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Configurations {

    private static ConfigurationProvider provider;

    private Configurations(ConfigurationProvider provider){
        this.provider = provider;
    }

    public static Configurations fromFiles(String... files){
        List<Path> paths = new ArrayList<>();
        Arrays.asList(files).forEach(file -> paths.add(Paths.get(file)));
        return fromFiles(paths);
    }

    public static Configurations fromFiles(Path... paths){
        return fromFiles(Arrays.asList(paths));
    }

    public static Configurations fromFiles(List<Path> paths){
        // Specify which files to load. Configuration from both files will be merged.
        ConfigFilesProvider configFilesProvider = () -> paths;
        // Use classpath repository as configuration store
        ConfigurationSource source = new ClasspathConfigurationSource(configFilesProvider);

        return createProvider(source);
    }

    public static Configurations fromUri(String uri) {
        // Select our sample git repository as the configuration source
        ConfigurationSource source = new GitConfigurationSourceBuilder()
                .withRepositoryURI(uri)
                .build();
        return createProvider(source);
    }

    private static Configurations createProvider(ConfigurationSource source){
        // Create provider
        ConfigurationProvider provider =  new ConfigurationProviderBuilder()
                .withConfigurationSource(source)
                .build();

        return new Configurations(provider);
    }


    public Properties obtainProperties(){
        return provider.allConfigurationAsProperties();
    }

    public <T> T obtainProperty(String key, Class<T> clazz) {
        return provider.getProperty(key, clazz);
    }

    public <T> T bind(String key, Class<T> clazz) {
        return provider.bind(key, clazz);
    }

}
