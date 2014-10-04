/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.utilint;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JVMSystemUtils {
    private static final String JAVA_VERSION_KEY = "java.version";
    private static final String JDK5_VERSION = "1.5";
    private static final String METHOD_NAME ="getSystemLoadAverage";
    private static final String JDK_VERSION = 
        System.getProperty(JAVA_VERSION_KEY);
    private static OperatingSystemMXBean bean;
    private static Method averageLoad;

    static {
        bean = ManagementFactory.getOperatingSystemMXBean();
        try {
            averageLoad = 
                bean.getClass().getMethod(METHOD_NAME, new Class[] {});
        } catch (NoSuchMethodException e) {
            /* Should never happen. */
        }
    }

    /*
     * Get the system load average for the last minute, return -1 for JDK5
     * because this method is not supported or there exists exceptions while
     * invocation.
     */
    public static double getSystemLoad() { 
        if (JDK_VERSION.startsWith(JDK5_VERSION)) {
            return -1;
        }

        /* Return the load only java version > 5.0. */
        double systemLoad = -1;
        try {
            systemLoad = new Double
                (averageLoad.invoke(bean, new Object[] {}).toString());
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch (InvocationTargetException e) {
            throw new IllegalStateException(e);
        }

        return systemLoad;
    }
}
