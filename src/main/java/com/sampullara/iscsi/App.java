package com.sampullara.iscsi;

import org.jscsi.target.Configuration;
import org.jscsi.target.TargetServer;

import java.io.IOException;

/**
 * Hello world!
 */
public class App {
  public static void main(String[] args) throws IOException {
    Configuration conf = new Configuration("");

    TargetServer targetServer = new TargetServer(conf);

  }
}
