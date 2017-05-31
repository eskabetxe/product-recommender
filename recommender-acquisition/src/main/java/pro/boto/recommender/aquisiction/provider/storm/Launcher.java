package pro.boto.recommender.aquisiction.provider.storm;


import org.apache.commons.lang.time.StopWatch;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {

    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private boolean runLocally;
    private String naming;
    private Config config;

    public Launcher(Boolean runLocally, Integer workers){
        this.runLocally = runLocally;
        this.naming = "recommender-acquisition";
        this.config = createConfiguration(runLocally, workers);
    }

    private Config createConfiguration(Boolean runLocally,Integer workers){
        Config config = new Config();

        config.setMaxSpoutPending(30);
        config.setMessageTimeoutSecs(1200);
        config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, "-Xmx2g");

     /*   if(!runLocally){
            config.put(Config.NIMBUS_SEEDS, Arrays.asList("172.25.0.10"));
            config.put(Config.NIMBUS_THRIFT_PORT,6627);
            System.setProperty("storm.jar", "/home/boto/develop/git-repos/github/product-recommender/recommender-storm/target/recommender-storm-1.0-SNAPSHOT.jar");
        }
*/
        if(this.runLocally){
            config.setDebug(false);
        }else{
            config.setDebug(false);
        }
        return config;
    }

    public void launchTopology(){
        try {
            if(this.runLocally){
                launchLocal();
            }else{
                launchRemote();
            }
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    private void launchRemote(){
        try {
            StormTopology topology = Topology.createTopology(naming);
            StormSubmitter.submitTopology(naming, config, topology);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void launchLocal(){
        LocalCluster cluster = new LocalCluster();
        try {
            StormTopology topology = Topology.createTopology(naming);
            cluster.submitTopology(naming, config, topology);
            StopWatch watch = new StopWatch();
            watch.start();
            Thread.sleep(100000);
        } catch (Exception e) {
            LOG.error(e.getLocalizedMessage(), e);
            throw new RuntimeException(e);
        } finally {
            cluster.shutdown();
        }
    }
}
