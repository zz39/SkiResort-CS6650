package com.cs6650;

import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


@WebServlet("/skiers/*")
// example: localhost:8080/Assignment1_war_exploded/skiers/3/seasons/2019/day/12/skier/9999
// schema: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}:
// example: http://54.84.188.93<ec2 public ip>:8080/Assignment1/skiers/3/seasons/2019/day/12/skier/9999
public class SkierServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();
    System.out.println(urlPath);

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts, false)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      res.getWriter().write("It works!");
    }
  }

  private boolean isUrlValid(String[] urlPath, Boolean get) {
    if (get) {
      return true;
    }
    else {
      String urlPath1 = urlPath[1];
      String urlPath3 = urlPath[3];
      String urlPath5 = urlPath[5];
      try {
        Integer.parseInt(urlPath1);
        Integer.parseInt(urlPath3);
        Integer.parseInt(urlPath5);
        return true;
      } catch(NumberFormatException e){
        return false;
      }
    }
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws ServletException, IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts, false)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      res.setStatus(HttpServletResponse.SC_CREATED);
      String body = req.getReader().lines().collect(Collectors.joining());

      if (body.isEmpty()) {
        res.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      }
      else {
        res.getWriter().write("POST ok!");
      }
    }
  }
}
