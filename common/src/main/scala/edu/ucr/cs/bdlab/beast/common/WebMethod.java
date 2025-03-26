package edu.ucr.cs.bdlab.beast.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
  * Annotates methods that handles a web request. The URL defines the URL path to access this method and
  * the format of any parameters given as part of it.
  */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface WebMethod{
  /**
   * the path on which this web method can be accessed from a web request. If not set, the name of the method will
   * be used.
   * @return the URL pattern of this method
   */
  String url() default "";

  /**
   * the order on which this method should be matched. The higher the number, the latter this
   * method will be considered while matching. Ties will be resolved using the alphabetical
   * order of the class#method name
   * @return the priority order
   */
  int order() default 1;

  /**
   * Whether this web method should be handled in asynchronous mode
   * @return {@code true} if this method should be handled asynchronously
   */
  boolean async() default false;

  /**
   * Can be optionally to limit the call of this services to given HTTP method
   * @return the HTTP method to use (if desired)
   */
  String method() default "";
}
