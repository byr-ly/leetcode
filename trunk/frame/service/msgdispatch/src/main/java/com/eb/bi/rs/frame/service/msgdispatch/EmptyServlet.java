package com.eb.bi.rs.frame.service.msgdispatch;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * An Empty, "do nothing servlet" to add to the context. Otherwise, the filters
 * will never kick in.
 */
public class EmptyServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        throw new IllegalStateException("unable to service request");
    }
}
