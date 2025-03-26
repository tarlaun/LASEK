package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import edu.ucr.cs.bdlab.test.JavaSparkTest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.DefaultHttpClient;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class BeastServerTest extends JavaSparkTest {

  static boolean method1Called = false;
  static boolean method2Called = false;
  static boolean method3Called = false;
  static int method3Param = 0;
  static boolean method4Called = false;
  static String method4Param = null;
  static boolean method5Called = false;
  static int method5ParamX = 0;
  static int method5ParamY = 0;
  static boolean method6GetCalled = false;
  static boolean method6PostCalled = false;
  static boolean method6PutCalled = false;
  static boolean method7Called = false;

  public static class FakeHandler extends AbstractWebHandler {

    public boolean makeSuccessful(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      response.setStatus(HttpServletResponse.SC_OK);
      PrintWriter writer = response.getWriter();
      writer.println("success");
      writer.close();
      return true;
    }

    @WebMethod
    public boolean method1(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      method1Called = true;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod2")
    public boolean method2(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      method2Called = true;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod3/{x}")
    public boolean method3(String target, HttpServletRequest request, HttpServletResponse response, int x) throws IOException {
      method3Called = true;
      method3Param = x;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod4/{x}-(\\d+)")
    public boolean method4(String target, HttpServletRequest request, HttpServletResponse response, String x) throws IOException {
      method4Called = true;
      method4Param = x;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod5/{x}-{y}")
    public boolean method5(String target, HttpServletRequest request, HttpServletResponse response,
                           int x, int y) throws IOException {
      method5Called = true;
      method5ParamX = x;
      method5ParamY = y;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod6", method = "post")
    public boolean method6Post(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      method6PostCalled = true;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod6", method = "get")
    public boolean method6Get(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      method6GetCalled = true;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod6", method = "put")
    public boolean method6Put(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      method6PutCalled = true;
      return makeSuccessful(target, request, response);
    }

    @WebMethod(url = "/callMethod7/", method = "get")
    public boolean method7(String target, HttpServletRequest request, HttpServletResponse response) throws IOException {
      method7Called = true;
      return makeSuccessful(target, request, response);
    }
  }

  public BeastServer startServer() {
    final BeastServer beastServer = new BeastServer();
    Random random = new Random();
    int attempts = 10;
    do {
      int port = random.nextInt(10000) + 10000;
      BeastOptions opts = new BeastOptions(false).set("port", port)
          .setBoolean("background", true);
      beastServer.setup(opts);
      beastServer.run(opts, null, null, sparkSession());
    } while (attempts-- > 0 && !beastServer.waitUntilStarted());
    if (!beastServer.isStarted())
      throw new RuntimeException("Error starting server");
    return beastServer;
  }

  public void testCallMethodByName() throws Exception {
    // Force the fake handler into the configuration
    Map<String, List<String>> beastConf = OperationHelper.readConfigurationXML("beast.xml");
    if (!beastConf.containsKey("WebHandlers"))
      beastConf.put("WebHandlers", new ArrayList<>());
    beastConf.get("WebHandlers").add(FakeHandler.class.getName());

    BeastServer beastServer = startServer();
    try {
      String prefix = String.format("http://127.0.0.1:%d", beastServer.port);
      // Try method1 defined by its name
      getRequest(prefix+"/method1");
      assertTrue("Method 1 should be called", method1Called);
      // Try method2 defined by a URL
      getRequest(prefix+"/callMethod2");
      assertTrue("Method 2 should be called", method2Called);
      // Try method3 defined by a URL with parameter
      getRequest(prefix+"/callMethod3/44");
      assertTrue("Method 3 should be called", method3Called);
      assertEquals(44, method3Param);
      // Try method4 defined by a URL with a parameter and regular expression
      getRequest(prefix+"/callMethod4/para-34234");
      assertTrue("Method 4 should be called", method4Called);
      assertEquals("para", method4Param);
      // Try method5 defined by a URL with two parameters
      getRequest(prefix+"/callMethod5/5555-66");
      assertTrue("Method 5 should be called", method5Called);
      assertEquals(5555, method5ParamX);
      assertEquals(66, method5ParamY);
    } finally {beastServer.stop(); beastServer.waitUntilStopped();}
  }

  public void testMethodWithVerbs() throws Exception {
    // Force the fake handler into the configuration
    Map<String, List<String>> beastConf = OperationHelper.readConfigurationXML("beast.xml");
    if (!beastConf.containsKey("WebHandlers"))
      beastConf.put("WebHandlers", new ArrayList<>());
    beastConf.get("WebHandlers").add(FakeHandler.class.getName());
    BeastServer beastServer = startServer();
    try {
      String prefix = String.format("http://127.0.0.1:%d", beastServer.port);
      // Try method6 that runs on POST
      postRequest(prefix+"/callMethod6");
      assertTrue(method6PostCalled);
      assertFalse(method6GetCalled);
      assertFalse(method6PutCalled);
      // Try method6 that runs on GET
      getRequest(prefix+"/callMethod6");
      assertTrue(method6GetCalled);
      assertFalse(method6PutCalled);
      // Try method6 that runs on PUT
      putRequest(prefix+"/callMethod6");
      assertTrue(method6PutCalled);
    } finally {beastServer.stop(); beastServer.waitUntilStopped();}
  }

  public void testIgnoreTrailingSlash() throws Exception {
    // Force the fake handler into the configuration
    Map<String, List<String>> beastConf = OperationHelper.readConfigurationXML("beast.xml");
    if (!beastConf.containsKey("WebHandlers"))
      beastConf.put("WebHandlers", new ArrayList<>());
    beastConf.get("WebHandlers").add(FakeHandler.class.getName());
    BeastServer beastServer = startServer();
    try {
      String prefix = String.format("http://127.0.0.1:%d", beastServer.port);
      // Try method2 without a trailing slash
      getRequest(prefix+"/callMethod2");
      assertTrue("Method 2 should be called without a trailing slash", method2Called);
      method2Called = false;
      // Try method2 with a trailing slash
      getRequest(prefix+"/callMethod2/");
      assertTrue("Method 2 should be called with a trailing slash", method2Called);
      method2Called = false;
      // Try method7 without a trailing slash
      getRequest(prefix+"/callMethod7");
      assertTrue("Method 7 should be called without a trailing slash", method7Called);
      method7Called = false;
      // Try method7 with a trailing slash
      getRequest(prefix+"/callMethod7/");
      assertTrue("Method 7 should be called with a trailing slash", method7Called);
      method7Called = false;
    } finally {beastServer.stop(); beastServer.waitUntilStopped();}
  }

  void getRequest(String path) throws IOException {
    URL url = new URL(path);
    try (InputStream inputStream = url.openStream()) {
      byte[] buffer = new byte[1024];
      while (inputStream.read(buffer) > 0) {}
    }
  }

  void postRequest(String path) throws IOException {
    HttpClient httpclient = new DefaultHttpClient();
    HttpPost httppost = new HttpPost(path);

    HttpResponse response = httpclient.execute(httppost);
    HttpEntity entity = response.getEntity();

    if (entity != null) {
      byte[] buffer = new byte[1024];
      try (InputStream inputStream = entity.getContent()) {
        while (inputStream.read(buffer) > 0) {}
      }
    }
  }

  void putRequest(String path) throws IOException {
    HttpClient httpclient = new DefaultHttpClient();
    HttpPut httppost = new HttpPut(path);

    HttpResponse response = httpclient.execute(httppost);
    HttpEntity entity = response.getEntity();

    if (entity != null) {
      byte[] buffer = new byte[1024];
      try (InputStream inputStream = entity.getContent()) {
        while (inputStream.read(buffer) > 0) {}
      }
    }
  }

}
