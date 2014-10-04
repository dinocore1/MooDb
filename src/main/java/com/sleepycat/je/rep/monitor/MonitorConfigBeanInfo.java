/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.je.rep.monitor;

import com.sleepycat.util.ConfigBeanInfoBase;

import java.beans.BeanDescriptor;
import java.beans.PropertyDescriptor;

public class MonitorConfigBeanInfo extends ConfigBeanInfoBase {

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return getBdescriptor(MonitorConfig.class);
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return getPdescriptor(MonitorConfig.class);
    }
}
