package com.sleepycat.je.sync.mobile;

import java.io.Serializable;

/**
 * Connection properties for Mobile Server.
 */
public class MobileConnectionConfig implements Serializable, Cloneable {

    /* URL for connecting to the remote mobile server. */
    private String url;

    /* Username and password for log on the mobile server. */
    private String userName;
    private char[] password;

    /* Encryption type used on transaferred data between server and client. */
    private String encryptionType;

    /* Whether allow create new DataSet. */
    private boolean allowNewDataSets;

    public MobileConnectionConfig setURL(String url) {
        this.url = url;

        return this;
    }

    public String getURL() {
        return url;
    }

    public MobileConnectionConfig setUserName(String userName) {
        this.userName = userName;

        return this;
    }

    public String getUserName() {
        return userName;
    }

    public MobileConnectionConfig setPassword(char[] password) {
        this.password = password;

        return this;
    }

    public char[] getPassword() {
        return password;
    }

    public MobileConnectionConfig setEncryptionType(String encryptionType) {
        this.encryptionType = encryptionType;

        return this;
    }

    public String getEncryptionType() {
        return encryptionType;
    }

    public MobileConnectionConfig setAllowNewDataSets
        (boolean allowNewDataSets) {
        this.allowNewDataSets = allowNewDataSets;

        return this;
    }

    public boolean getAllowNewDataSets() {
        return allowNewDataSets;
    }

    @Override
    public MobileConnectionConfig clone() {
        try {
            return (MobileConnectionConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }
}
