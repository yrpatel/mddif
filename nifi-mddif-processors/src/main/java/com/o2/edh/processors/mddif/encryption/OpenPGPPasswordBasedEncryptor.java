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
package com.o2.edh.processors.mddif.encryption;

import com.o2.edh.processors.mddif.encryption.MDDIFEncryptContent.Encryptor;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.PBEDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.PGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.PGPKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEDataDecryptorFactoryBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEKeyEncryptionMethodGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.bouncycastle.openpgp.PGPUtil.getDecoderStream;

public class OpenPGPPasswordBasedEncryptor implements Encryptor {
    private static final Logger logger = LoggerFactory.getLogger(OpenPGPPasswordBasedEncryptor.class);

    private String algorithm;
    private String provider;
    private char[] password;
    private String filename;
    private Integer cipher;

    public OpenPGPPasswordBasedEncryptor(final String algorithm, final Integer cipher, final String provider, final char[] passphrase, final String filename) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.password = passphrase;
        this.filename = filename;
        this.cipher = cipher;
    }

    @Override
    public StreamCallback getEncryptionCallback() throws Exception {
        return new OpenPGPEncryptCallback(algorithm, cipher, provider, password, filename);
    }

    @Override
    public StreamCallback getDecryptionCallback() throws Exception {
        return new OpenPGPDecryptCallback(provider, password);
    }

    private static class OpenPGPDecryptCallback implements StreamCallback {

        private String provider;
        private char[] password;

        OpenPGPDecryptCallback(final String provider, final char[] password) {
            this.provider = provider;
            this.password = password;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            InputStream pgpin = getDecoderStream(in);
            JcaPGPObjectFactory pgpFactory = new JcaPGPObjectFactory(pgpin);

            Object obj = pgpFactory.nextObject();
            if (!(obj instanceof PGPEncryptedDataList)) {
                obj = pgpFactory.nextObject();
                if (!(obj instanceof PGPEncryptedDataList)) {
                    throw new ProcessException("Invalid OpenPGP data");
                }
            }
            PGPEncryptedDataList encList = (PGPEncryptedDataList) obj;

            obj = encList.get(0);
            if (!(obj instanceof PGPPBEEncryptedData)) {
                throw new ProcessException("Invalid OpenPGP data");
            }
            PGPPBEEncryptedData encryptedData = (PGPPBEEncryptedData) obj;

            try {
                final PGPDigestCalculatorProvider digestCalculatorProvider = new JcaPGPDigestCalculatorProviderBuilder().setProvider(provider).build();
                final PBEDataDecryptorFactory decryptorFactory = new JcePBEDataDecryptorFactoryBuilder(digestCalculatorProvider).setProvider(provider).build(password);
                InputStream clear = encryptedData.getDataStream(decryptorFactory);

                JcaPGPObjectFactory pgpObjectFactory = new JcaPGPObjectFactory(clear);

                obj = pgpObjectFactory.nextObject();
                if (obj instanceof PGPCompressedData) {
                    PGPCompressedData compressedData = (PGPCompressedData) obj;
                    pgpObjectFactory = new JcaPGPObjectFactory(compressedData.getDataStream());
                    obj = pgpObjectFactory.nextObject();
                }

                PGPLiteralData literalData = (PGPLiteralData) obj;
                InputStream plainIn = literalData.getInputStream();
                final byte[] buffer = new byte[com.o2.edh.processors.mddif.encryption.PGPUtil.BLOCK_SIZE];
                int len;
                while ((len = plainIn.read(buffer)) >= 0) {
                    out.write(buffer, 0, len);
                }

                if (encryptedData.isIntegrityProtected()) {
                    if (!encryptedData.verify()) {
                        throw new PGPException("Integrity check failed");
                    }
                } else {
                    logger.warn("No message integrity check");
                }
            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }
        }
    }

    private static class OpenPGPEncryptCallback implements StreamCallback {

        private String algorithm;
        private String provider;
        private char[] password;
        private String filename;
        private Integer cipher;

        OpenPGPEncryptCallback(final String algorithm, final Integer cipher, final String provider, final char[] password, final String filename) {
            this.algorithm = algorithm;
            this.provider = provider;
            this.password = password;
            this.filename = filename;
            this.cipher = cipher;
        }

        @Override
        public void process(InputStream in, OutputStream out) throws IOException {
            try {
                PGPKeyEncryptionMethodGenerator encryptionMethodGenerator = new JcePBEKeyEncryptionMethodGenerator(password).setProvider(provider);
                com.o2.edh.processors.mddif.encryption.PGPUtil.encrypt(in, out, algorithm, provider, cipher, filename, encryptionMethodGenerator);
            } catch (Exception e) {
                throw new ProcessException(e.getMessage());
            }
        }
    }
}
