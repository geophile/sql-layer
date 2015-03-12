/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2009-2015 FoundationDB, LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.foundationdb.sql.ui;

import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;

import javax.swing.JFrame;
import javax.swing.SwingUtilities;

import java.io.PrintStream;

import com.google.inject.Inject;

public class SwingConsoleServiceImpl implements SwingConsoleService, Service {
    private final ServiceManager serviceManager;
    private final PrintStream origOut = System.out;
    private SwingConsole console;

    @Inject
    public SwingConsoleServiceImpl(ServiceManager serviceManager) {
        this.serviceManager = serviceManager;
    }

    @Override
    public final void start() {
        if (console == null) {
            console = new SwingConsole(serviceManager);
            PrintStream ps = console.openPrintStream(true);
            System.setOut(ps);
            ps.flush(); // Pick up output from before started.
        }
        show();
    }

    @Override
    public final void stop() {
        // If we stop while starting, that means some other service has problems.
        // Let the user see them before removing the console.
        switch (serviceManager.getState()) {
        case STARTING:
            return;
        }
        final JFrame console = this.console;
        this.console = null;
        System.setOut(origOut);
        if (console != null) {
            SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        console.dispose();
                    }
                });
        }
    }
    
    @Override
    public void crash() {
        stop();
    }

    @Override
    public void show() {
        setVisible(true);
    }

    @Override
    public void hide() {
        setVisible(true);
    }
    
    private void setVisible(final boolean visible) {
        final JFrame console = this.console;
        if (console != null) {
            SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        console.setVisible(visible);
                    }
                });
        }
        else
            throw new IllegalStateException("No frame to show / hide.");
    }

    @Override
    public PrintStream getPrintStream() {
        return console.getPrintStream();
    }

}
