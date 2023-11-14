/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.mqtt.example;

import org.apache.rocketmq.mqtt.common.util.HmacSHA1Util;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import javax.crypto.Cipher;
import javax.crypto.EncryptedPrivateKeyInfo;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Collection;

public class SSLUtils {
    public static SSLContext getSSLContext(String caPath, String crtPath, String keyPath, String password) throws Exception {

        CertificateFactory cAf = CertificateFactory.getInstance("X.509");
        FileInputStream caIn = new FileInputStream(caPath);
        X509Certificate ca = (X509Certificate) cAf.generateCertificate(caIn);
        KeyStore caKs = KeyStore.getInstance("JKS");
        caKs.load(null, password.toCharArray());
        caKs.setCertificateEntry("ca1", ca);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance("PKIX");
        tmf.init(caKs);
        caIn.close();

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        FileInputStream crtIn = new FileInputStream(crtPath);
        Collection<? extends Certificate> certs = cf.generateCertificates(crtIn);
        X509Certificate[] x509Certs = new X509Certificate[certs.size()];
        int i = 0;
        for (Certificate cert : certs) {
            x509Certs[i++] = (X509Certificate) cert;
        }
        crtIn.close();

        KeyStore ks = KeyStore.getInstance("JKS");
        ks.load(null, password.toCharArray());
        ks.setCertificateEntry("certificate3", x509Certs[0]);

        Key privateKey = getPrivateKey(new File(keyPath));
        ks.setKeyEntry("private-key", privateKey, password.toCharArray(), x509Certs);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance("PKIX");
        kmf.init(ks, password.toCharArray());

        SSLContext context = SSLContext.getInstance("TLSv1.2");
        KeyManager[] kms = kmf.getKeyManagers();
        TrustManager[] tms = tmf.getTrustManagers();
        context.init(kms, tms, new SecureRandom());
        return context;
    }

    public static Key getPrivateKey(File file) throws InvalidAlgorithmParameterException, NoSuchPaddingException, InvalidKeyException {
        if (file == null) {
            return null;
        }
        PrivateKey privKey = null;
        PemReader pemReader = null;
        try {
            pemReader = new PemReader(new FileReader(file));
            PemObject pemObject = pemReader.readPemObject();
            byte[] pemContent = pemObject.getContent();
            PKCS8EncodedKeySpec encodedKeySpec = generateKeySpec(null, pemContent);
            try {
                return KeyFactory.getInstance("RSA").generatePrivate(encodedKeySpec);
            } catch (InvalidKeySpecException ignore) {
                try {
                    return KeyFactory.getInstance("DSA").generatePrivate(encodedKeySpec);
                } catch (InvalidKeySpecException ignore2) {
                    try {
                        return KeyFactory.getInstance("EC").generatePrivate(encodedKeySpec);
                    } catch (InvalidKeySpecException e) {
                        throw new InvalidKeySpecException("Neither RSA, DSA nor EC worked", e);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("read private key fail,the reason is the file not exist");
            e.printStackTrace();
        } catch (IOException | InvalidKeySpecException | NoSuchAlgorithmException e) {
            System.out.println("read private key fail,the reason is :" + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (pemReader != null) {
                    pemReader.close();
                }
            } catch (IOException e) {
                System.out.println(e.getMessage());
            }
        }
        return privKey;
    }

    public static PKCS8EncodedKeySpec generateKeySpec(char[] password, byte[] key)
            throws IOException, NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeySpecException,
            InvalidKeyException, InvalidAlgorithmParameterException {

        if (password == null) {
            return new PKCS8EncodedKeySpec(key);
        }

        EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = new EncryptedPrivateKeyInfo(key);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(encryptedPrivateKeyInfo.getAlgName());
        PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
        SecretKey pbeKey = keyFactory.generateSecret(pbeKeySpec);

        Cipher cipher = Cipher.getInstance(encryptedPrivateKeyInfo.getAlgName());
        cipher.init(Cipher.DECRYPT_MODE, pbeKey, encryptedPrivateKeyInfo.getAlgParameters());

        return encryptedPrivateKeyInfo.getKeySpec(cipher);
    }

    public static MqttConnectOptions buildMqttConnectOptions(String clientId, String caPath, String deviceCrtPath,
                                                             String deviceKyPath, String password) throws Exception {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(false);
        connOpts.setKeepAliveInterval(60);
        connOpts.setAutomaticReconnect(true);
        connOpts.setMaxInflight(10000);
        connOpts.setUserName(System.getenv("username"));
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, System.getenv("password")).toCharArray());

        SSLContext ctx = SSLUtils.getSSLContext(caPath, deviceCrtPath, deviceKyPath, password);
        connOpts.setSocketFactory(ctx.getSocketFactory());
        return connOpts;
    }
}
