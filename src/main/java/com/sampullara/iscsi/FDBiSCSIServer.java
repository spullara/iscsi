package com.sampullara.iscsi;

import com.foundationdb.Database;
import com.foundationdb.FDB;
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
    Database db = FDB.selectAPIVersion(200).open();
    Target target = new Target("iqn.2014-11.com.sampullara:storage:fdbiscsi", "fdbiscsi", new FDBStorage(db, "fdbiscsi"));
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
