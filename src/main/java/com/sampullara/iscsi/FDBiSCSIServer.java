package com.sampullara.iscsi;

import org.jscsi.target.Configuration;
import org.jscsi.target.Target;
import org.jscsi.target.TargetServer;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FDBiSCSIServer {
  public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
    Target target = new Target("com.sampullara.iscsi", "fdbiscsi", new FDBStorage());
    Configuration conf = new Configuration("");
    List<Target> targets = conf.getTargets();
    targets.add(target);
    TargetServer targetServer = new TargetServer(conf);

    //Getting an Executor
    ExecutorService threadPool = Executors.newSingleThreadExecutor();

    //Starting the target
    threadPool.submit(targetServer);
  }
}
