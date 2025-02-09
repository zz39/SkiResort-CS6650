package com.cs6650;

import java.io.*;
import javax.servlet.*;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.*;

@WebServlet("/hello")
public class HelloWorldServlet extends HttpServlet {

  private String msg;

  public void init() throws ServletException {
    // Initialization
    msg = "Hello World my friend!";
  }

  // handle a GET request
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {

    // Set response content type to text
    response.setContentType("text/html");

    // sleep for 1000ms. You can vary this value for different tests
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // Send the response
    PrintWriter out = response.getWriter();
    out.println("<h1>" + msg + "</h1>");
  }

  public void destroy() {
    // nothing to do here
  }
}