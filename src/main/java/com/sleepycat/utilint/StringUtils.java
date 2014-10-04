/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2012 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package com.sleepycat.utilint;

import java.io.UnsupportedEncodingException;

public class StringUtils {

    /**
     * In all src and test code, the String(byte[], ...) constructor and
     * String.getBytes method must always be passed a charset name, to avoid
     * portability issues.  Otherwise, portability issues will occur when
     * running on a JVM plataform with a non-western default charset, the
     * EBCDIC encoding (on z/OS), etc.  [#20967]
     * <p>
     * In most cases, the UTF8 or ASCII charset should be used for portability.
     * UTF8 should be used when any character may be represented.  ASCII can be
     * used when all characters are in the ASCII range.  The default charset
     * should only be used when handling user-input data directly, e.g.,
     * console input/output or user-visible files.
     * <p>
     * Rather than using getBytes() and String() directly, the methods here are
     * used to avoid having to clutter code with a catch for
     * java.io.UnsupportedEncodingException, which should never be thrown for
     * the "UTF-8" or "US-ASCII" charsets.
     */
    public static byte[] toUTF8(String str) {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            /* Should never happen. */
            throw new RuntimeException(e);
        }
    }

    public static String fromUTF8(byte[] bytes) {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            /* Should never happen. */
            throw new RuntimeException(e);
        }
    }

    public static String fromUTF8(byte[] bytes, int offset, int len) {
        try {
            return new String(bytes, offset, len, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            /* Should never happen. */
            throw new RuntimeException(e);
        }
    }
    
    public static byte[] toASCII(String str) {
        try {
            return str.getBytes("US-ASCII");
        } catch (UnsupportedEncodingException e) {
            /* Should never happen. */
            throw new RuntimeException(e);
        }
    }

    public static String fromASCII(byte[] bytes) {
        try {
            return new String(bytes, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            /* Should never happen. */
            throw new RuntimeException(e);
        }
    }

    public static String fromASCII(byte[] bytes, int offset, int len) {
        try {
            return new String(bytes, offset, len, "US-ASCII");
        } catch (UnsupportedEncodingException e) {
            /* Should never happen. */
            throw new RuntimeException(e);
        }
    }

    /**
     * NOTE: the following definitions cannot be used until we require Java 1.6
     * for all JE usage, including Android.
     * 
     * In all src and test code, the String(byte[], ...) constructor and
     * String.getBytes method must always be passed a Charset, to avoid
     * portability issues.  Otherwise, portability issues will occur when
     * running on a JVM plataform with a non-western default charset, the
     * EBCDIC encoding (on z/OS), etc.  [#20967]
     * <p>
     * In most cases, the UTF8 or ASCII charset should be used for portability.
     * UTF8 should be used when any character may be represented.  ASCII can be
     * used when all characters are in the ASCII range.  The default charset
     * should only be used when handling user-input data directly, e.g.,
     * console input/output or user-visible files.
     * <p>
     * Rather than passing the charset as a string (getBytes("UTF-8")), the
     * Charset objects defined here should be passed (getBytes(UTF8)).  Not
     * only is using a Charset object slightly more efficient because it avoids
     * a lookup, even more importantly it avoids having to clutter code with a
     * catch for java.io.UnsupportedEncodingException, which should never be
     * thrown for the "UTF-8" or "US-ASCII" charsets.
    import java.nio.charset.Charset;
    public static final Charset UTF8 = Charset.forName("UTF-8");
    public static final Charset ASCII = Charset.forName("US-ASCII");
     */
}
