package sahibul;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import sahibul.bolts.LearnStormBolt;
import sahibul.spouts.LearnStormSpout;

/**
 * Belajar Storm
 * bareng sahibul_nf dan si_syin
 *
 */
public class LearnStormTopology
{
    public static void main( String[] args )
    {
        // membuat instanciasi class TopologyBuilder
        TopologyBuilder builder = new TopologyBuilder();
        // set class spout nya
        builder.setSpout("LearnStormSpout", new LearnStormSpout());
        // set class bolt nya
        builder.setBolt("LearnStormBolt", new LearnStormBolt(), 1).shuffleGrouping("LearnStormSpout");

        Config config = new Config();
        config.setDebug(true);

        // membuat instansiasi class LocalCluster
        // untuk menjalankan topology di mode local
        LocalCluster cluster = new LocalCluster();
        // submit LearnStormTopology ke topology local
        cluster.submitTopology("LearnStormTopology", config, builder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            System.out.println("Ada Kesalahan ni : " + e);
        }

        // kill LearnStormTopology
        cluster.killTopology("LearnStormTopology");
        // dan shutdown storm cluster
        cluster.shutdown();

    }
}
