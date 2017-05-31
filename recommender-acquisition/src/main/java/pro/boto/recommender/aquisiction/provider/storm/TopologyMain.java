package pro.boto.recommender.aquisiction.provider.storm;


public class TopologyMain {

    public static void main(String[] args) {
        TopologyMain main = new TopologyMain();
        main.processMain();
    }

    private void processMain() {
        Launcher topology = new Launcher(true, 1);
        topology.launchTopology();
    }


}
