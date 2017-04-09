<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
<%@ page contentType="text/html; charset=UTF-8" import="javax.servlet.*"
	import="javax.servlet.http.*" import="java.io.*" import="java.util.*"
    import="java.text.DecimalFormat" import="java.text.Format" import="org.apache.hama.bsp.*"
    import="org.apache.hama.util.*" import="org.apache.hadoop.http.HtmlQuoting"%>
<%!private static final long serialVersionUID = 1L;%>
<%
  BSPMaster tracker = (BSPMaster) application
      .getAttribute("bsp.master");
  String idString = request.getParameter("jobid");
  JobStatus status = tracker.getJobStatus(BSPJobID.forName(idString));
  JobStatus.State state = status.getState();
%>

<html>
<head>
<title>Hama BSP Job Summary</title>
<link rel="stylesheet" href="/static/hama.css" />
</head>
<body>
  <h1><%=status.getName()%></h1>
  <div class="block-detail">
    <ul>
      <li><span>State : </span><%=state.toString() %></li>
  </div>
  <br/>
  <div class="block-list">
  <table border="1" cellpadding="6" cellspacing="0">
    <tr>
      <th>Name</th>
      <th>User</th>
      <th>SuperSteps</th>
      <th>Tasks</th>
      <th>StartTime</th>
      <th>FinishTime</th>
      <th>Job Logs</th>      
    </tr>

    <tr>
      <td><%=status.getName() %></td>
      <td><%=status.getUsername() %></td>
      <td><%=status.getSuperstepCount() %></td>
      <td><%=status.getNumOfTasks() %></td>
      <td><%=new Date(status.getStartTime()).toString() %></td>
      <td>
        <% if(status.getFinishTime() != 0L) {out.write(new Date(status.getFinishTime()).toString());} %>
      </td>
      <td><a href="/logView?dir=tasklogs/<%=idString%>&jobId=<%=idString%>&type=tasklist">view</a></td>      
    </tr>

  </table>
  </div>
  <br/>
  <div class="block-list">  
  <table border="1" cellpadding="6" cellspacing="0">
    <tr>
      <th><br/></th>
      <th>Counter</th>
      <th>Total</th>
    </tr>
    <%
    Counters counters = status.getCounter();
    if (counters == null) {
      counters = new Counters();
    }

    for (String groupName : counters.getGroupNames()) {
      Counters.Group group = counters.getGroup(groupName);

      Format decimal = new DecimalFormat();

      boolean isFirst = true;
      for (Counters.Counter counter : group) {
        String name = counter.getDisplayName();
        String value = decimal.format(counter.getCounter());
        %>
        <tr>
          <%
          if (isFirst) {
            isFirst = false;
            %>
            <td rowspan="<%=group.size()%>">
            <%=HtmlQuoting.quoteHtmlChars(group.getDisplayName())%></td>
            <%
          }
          %>
          <td><%=HtmlQuoting.quoteHtmlChars(name)%></td>
          <td align="right"><%=value%></td>
        </tr>
        <%
      }
    }
    %>
  </table>
  </div>
  <p/>
  <hr>
  <a href="bspmaster.jsp"><i>Back to BSPMaster</i></a>

  <%
    out.println(BSPServletUtil.htmlFooter());
  %>