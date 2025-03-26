/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.util;

import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.common.JCLIOperation;
import edu.ucr.cs.bdlab.beast.common.WebMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.thread.ThreadPool;
import scala.Tuple2;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@OperationMetadata(
  shortName = "server",
  description = "Starts the web UI server for Beast",
  inputArity = "0",
  outputArity = "0")
/**
 * Starts an HTTP server that hosts all configured Beast web-based operations from command line.
 */
public class BeastServer extends AbstractHandler implements JCLIOperation {
  private static final Log LOG = LogFactory.getLog(BeastServer.class);

  @OperationParam(
    description = "The port to start the web UI server on",
    required = false,
    defaultValue = "8890"
  )
  public static final String Port = "port";

  @OperationParam(
    description = "Number of threads to start on the server",
    required = false,
    defaultValue = "2*num cores"
  )
  public static final String NumThreads = "threads";

  @OperationParam(
    description = "Run the server in the background",
    required = false,
    defaultValue = "false"
  )
  public static final String Background = "background";

  /**The port to start the server on*/
  protected int port;

  /**User options given at the application startup*/
  private BeastOptions opts;

  /**A hashmap for keeping one instance of web handlers that were instantiated*/
  protected final Map<Class<? extends AbstractWebHandler>, AbstractWebHandler> webHandlers = new HashMap<>();

  static class WebMethodInfo {
    final Pattern pattern;
    final Class< ? extends AbstractWebHandler> webHandlerClass;
    final Method javaMethod;
    final WebMethod methodAnnotation;

    public WebMethodInfo(Pattern p, Class< ? extends AbstractWebHandler> w, Method m, WebMethod a) {
      this.pattern = p;
      this.webHandlerClass = w;
      this.javaMethod = m;
      this.methodAnnotation = a;
    }
  }

  /**The web methods ordered by their precedence*/
  protected WebMethodInfo[] webMethods;

  /**Catches parameters defined in the URL regular expression*/
  protected static final Pattern ParameterName = Pattern.compile("\\{\\w+\\}");

  /**A regular expression for each parameter type given its class name*/
  protected static final Map<Class, Pattern> ParameterTypeRegularExpression = new HashMap<>();

  static {

    Pattern intRegExp = Pattern.compile("[\\-\\+]?\\d+");
    ParameterTypeRegularExpression.put(Integer.class, intRegExp);
    ParameterTypeRegularExpression.put(int.class, intRegExp);
    ParameterTypeRegularExpression.put(Long.class, intRegExp);
    ParameterTypeRegularExpression.put(long.class, intRegExp);
    ParameterTypeRegularExpression.put(Short.class, intRegExp);
    ParameterTypeRegularExpression.put(short.class, intRegExp);
    ParameterTypeRegularExpression.put(Byte.class, intRegExp);
    ParameterTypeRegularExpression.put(byte.class, intRegExp);
    ParameterTypeRegularExpression.put(String.class, Pattern.compile(".+"));
    ParameterTypeRegularExpression.put(Object.class, Pattern.compile(".+"));
  }

  /**Spark context to pass to web handlers*/
  protected JavaSparkContext sc;

  /** The SparkSession by the underlying application */
  protected SparkSession ss;

  @Override public void setup(BeastOptions opts) {
    this.opts = opts;
    this.port = opts.getInt(Port, 8890);

    // Retrieve a list of all web handlers and their methods
    webMethods = loadWebMethods();
  }

  protected WebMethodInfo[] loadWebMethods() {
    List<Tuple2<Integer, WebMethodInfo>> webMethods = new ArrayList<>();
    List<String> handlerClassNames = OperationHelper.readConfigurationXML("beast.xml").get("WebHandlers");
    if (handlerClassNames == null)
      return null;
    for (String handlerClassName : handlerClassNames) {
      try {
        Class<? extends AbstractWebHandler> handlerClass = Class.forName(handlerClassName).asSubclass(AbstractWebHandler.class);
        Method[] methods = handlerClass.getMethods();
        for (Method method : methods) {
          // Check if it has the WebMethod annotation
          WebMethod webMethodAnnotation = method.getAnnotation(WebMethod.class);
          if (webMethodAnnotation != null) {
            String matchURL = webMethodAnnotation.url().length() > 0 ? webMethodAnnotation.url() : "/" + method.getName();
            // Remove the trailing slash
            if (matchURL.endsWith("/"))
              matchURL = matchURL.substring(0, matchURL.length() - 1);
            // Search and replace parameters with their corresponding matching regular expressions
            Matcher parameterFinder = ParameterName.matcher(matchURL);
            int iParameter = 0;
            // How many additional characters were added to the matchURL while replacing parameters names with regexp
            int diff = 0;
            while (parameterFinder.find()) {
              if (iParameter >= method.getParameters().length)
                throw new RuntimeException(String.format(
                    "Parameter '%s' defined in the match URL does not have a corresponding method parameter",
                    parameterFinder.group()));
              Pattern parameterRegExp = ParameterTypeRegularExpression.get(method.getParameters()[iParameter + 3].getType());
              if (parameterRegExp == null)
                throw new RuntimeException(String.format("Parameters of type '%s' cannot be automatically matched",
                    method.getParameters()[iParameter + 3].getType().getName()));
              // Replace the parameter with the corresponding capturing regular expression
              String capturingRegexp = String.format("(?<arg%d>%s)", iParameter, parameterRegExp.toString());
              int oldLength = matchURL.length();
              matchURL = matchURL.substring(0, parameterFinder.start() + diff) +
                  capturingRegexp +
                  matchURL.substring(parameterFinder.end() + diff);
              diff += matchURL.length() - oldLength;
              iParameter++;
            }
            int order = webMethodAnnotation.order();
            webMethods.add(new Tuple2<>(order, new WebMethodInfo(Pattern.compile(matchURL), handlerClass, method, webMethodAnnotation)));
          }
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
    }
    webMethods.sort((m1, m2) -> {
      if (m1._1() != m2._1())
        return m1._1() - m2._1();
      return m1._2().javaMethod.getName().compareTo(m2._2().javaMethod.getName());
    });
    WebMethodInfo[] finalResult = new WebMethodInfo[webMethods.size()];
    for (int $i = 0; $i < webMethods.size(); $i++) {
      finalResult[$i] = webMethods.get($i)._2();
    }
    return finalResult;
  }

  /**
   * Waits until the server starts or fails to start.
   * @return {@code ture} if the server started correctly and {@code false} if it fails to start.
   */
  public boolean waitUntilStarted() {
    synchronized(this) {
      while (this.isStarting()) {
        try {
          this.wait(1000);
        } catch (InterruptedException e) {
          return false;
        }
      }
    }
    // No error happened
    return this.isStarted();
  }

  public void waitUntilStopped() {
    synchronized(this) {
      while (!this.isStopped()) {
        try {
          this.wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }

  @Override
  public void handle(String target, HttpServletRequest request, HttpServletResponse response, int d) {
    long t1 = System.nanoTime();
    try {
      LOG.info(String.format("Received request '%s'", target));
      // Bypass cross-site scripting (XSS)
      response.addHeader("Access-Control-Allow-Origin", "*");
      response.addHeader("Access-Control-Allow-Credentials", "true");
      // Remove the trailing slash before matching any patterns
      if (target.endsWith("/"))
        target = target.substring(0, target.length() -1);
      // Try to match the response with one of the configured handle functions
      Matcher matcher = null;
      boolean matchFound = false;
      int i = 0;
      String requestMethod = request.getMethod();
      do {
        if (webMethods[i].methodAnnotation.method().isEmpty() ||
            requestMethod.equalsIgnoreCase(webMethods[i].methodAnnotation.method())) {
        matcher = webMethods[i].pattern.matcher(target);
          matchFound = matcher.matches();
        }
      } while (!matchFound && ++i < webMethods.length);
      if (i < webMethods.length) {
        Class<? extends AbstractWebHandler> matchingWebHandler = webMethods[i].webHandlerClass;
        Method matchingMethod = webMethods[i].javaMethod;
        // Call the method
        AbstractWebHandler webHandler = webHandlers.get(matchingWebHandler);
        if (webHandler == null) {
          // Initialize the web handler for the first time
          try {
            webHandler = matchingWebHandler.newInstance();
            webHandler.setup(ss, opts);
            webHandlers.put(matchingWebHandler, webHandler);
          } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
          }
        }
        // Extract method parameters from the matcher
        Object[] parameters = new Object[matchingMethod.getParameterCount()];
        parameters[0] = target;
        parameters[1] = request;
        parameters[2] = response;
        for (int $i = 3; $i < parameters.length; $i++) {
          // Additional parameter after the three required parameters
          String parameterValueStr = matcher.group(String.format("arg%d", $i - 3));
          Class<?> parameterType = matchingMethod.getParameterTypes()[$i];
          Object parameterValue = parseValue(parameterValueStr, parameterType);
          parameters[$i] = parameterValue;
        }
        // Handle synchronously
        Boolean handled = (Boolean) matchingMethod.invoke(webHandler, parameters);
        long t2 = System.nanoTime();
        if (handled) {
          ((Request) request).setHandled(handled);
          LOG.info(String.format("Request %s '%s' handled in %f seconds", requestMethod, target, (t2 - t1) * 1E-9));
        } else {
          LOG.info(String.format("Request %s '%s' not found in %f seconds", requestMethod, target, (t2 - t1) * 1E-9));
        }
      } else {
        // No matching method. Return 404 not found
        ((Request) request).setHandled(false);
        response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    } catch (Exception e) {
      // Absorb and neglect any exception that happens during the request to avoid killing the thread
      long t2 = System.nanoTime();
      try {
        LOG.warn(String.format("Error in request '%s' after %f seconds", target, (t2-t1)*1E-9), e);
        AbstractWebHandler.reportError(response, String.format("Error in request '%s' after %f seconds", target, (t2-t1)*1E-9), e);
        ((Request) request).setHandled(true);
      } catch (IOException ex) {
        LOG.error("Error reporting the error", e);
        LOG.error(ex);
      }
    }
  }

  /**
   * Parses the given string value according to a specific type (class).
   * @param valueStr the string representation of the value
   * @param type the expected type of the value
   * @return an instance of {@code type} that has the value of {@code valueStr}
   */
  protected static Object parseValue(String valueStr, Class<?> type) {
    Object parameterValue;
    switch (type.getName()) {
      case "java.lang.Byte":
      case "byte":
        parameterValue = Byte.parseByte(valueStr);
        break;
      case "java.lang.Short":
      case "short":
        parameterValue = Short.parseShort(valueStr);
        break;
      case "java.lang.Integer":
      case "int":
        parameterValue = Integer.parseInt(valueStr);
        break;
      case "java.lang.Long":
      case "long":
        parameterValue = Long.parseLong(valueStr);
        break;
      case "java.lang.String":
        parameterValue = valueStr;
        break;
      default:
        throw new RuntimeException(String.format("Cannot parse type '%s'", type.getName()));
    }
    return parameterValue;
  }

  /**
    * Run the main function using the given user command-line options and spark context
    *
    * @param opts user options for configuring the operation
    * @param ss   the Spark session used to run the operation
    * @return an optional result of this operation
    */
  @Override
  public Object run(BeastOptions opts, String[] inputs, String[] outputs, SparkSession ss) {
    try {
      this.ss = ss;
      this.sc = JavaSparkContext.fromSparkContext(ss.sparkContext());
      // Since the threads are not typically CPU bound, we create more threads than the available processors
      int defaultNumThreads = Runtime.getRuntime().availableProcessors() * 2;
      int numThreads = opts.getInt(NumThreads, defaultNumThreads);
      LOG.info(String.format("Creating a thread pool of %d threads", numThreads));
      ThreadPool threadPool = new QueuedThreadPool(numThreads);
      // The Jetty server
      Server server = new Server(port);
      server.setThreadPool(threadPool);
      server.setHandler(this);
      server.start();
      LOG.info(String.format("Started the server on port %d", port));
      if (!opts.getBoolean(Background, false))
        server.join();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public Object run(BeastOptions opts, String[] inputs, String[] outputs, JavaSparkContext sc) {
    throw new RuntimeException("Not supported. Use run(BeastOptions, String[], String[], SparkSession)");
  }

  @Override
  public void addDependentClasses(BeastOptions opts, Stack<Class<?>> classes) {
    List<String> handlerClassNames = OperationHelper.readConfigurationXML("beast.xml").get("WebHandlers");
    if (handlerClassNames == null)
      return;
    for (String handlerClassName : handlerClassNames) {
      try {
        Class<? extends AbstractWebHandler> handlerClass = Class.forName(handlerClassName).asSubclass(AbstractWebHandler.class);
        classes.push(handlerClass);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Error instantiating the class object", e);
      }
    }
  }
}
