package com.o2.edh.processors.mddif.encryption;

import java.io.*;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Date;
import java.util.Iterator;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.bc.BcPGPObjectFactory;
import org.bouncycastle.openpgp.operator.*;
import org.bouncycastle.openpgp.operator.bc.*;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;

public class MDDIFPGPUtils {

    private static final int[] MASTER_KEY_CERTIFICATION_TYPES = new int[] { 19, 18, 17, 16 };

    public static PGPPublicKey readPublicKey(InputStream in) throws IOException, PGPException {
        KeyFingerPrintCalculator fingerCalc = new JcaKeyFingerprintCalculator();
        PGPPublicKeyRingCollection keyRingCollection = new PGPPublicKeyRingCollection(org.bouncycastle.openpgp.PGPUtil.getDecoderStream(in), new BcKeyFingerprintCalculator());
        PGPPublicKey publicKey = null;
        Iterator<PGPPublicKeyRing> rIt = keyRingCollection.getKeyRings();
        while (publicKey == null && rIt.hasNext()) {
            PGPPublicKeyRing kRing = rIt.next();
            Iterator<PGPPublicKey> kIt = kRing.getPublicKeys();
            while (publicKey == null && kIt.hasNext()) {
                PGPPublicKey key = kIt.next();
                if (key.isEncryptionKey())
                    publicKey = key;
            }
        }
        if (publicKey == null)
            throw new IllegalArgumentException("Can't find public key in the key ring.");
        if (!isForEncryption(publicKey))
            throw new IllegalArgumentException("KeyID " + publicKey.getKeyID() + " not flagged for encryption.");
        return publicKey;
    }


    public static PGPPrivateKey findPrivateKey(InputStream keyIn, long keyID, char[] pass) throws IOException, PGPException, NoSuchProviderException {
        KeyFingerPrintCalculator fingerCalc = new JcaKeyFingerprintCalculator();
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(org.bouncycastle.openpgp.PGPUtil.getDecoderStream(keyIn), fingerCalc);
        return findPrivateKey(pgpSec.getSecretKey(keyID), pass);
    }

    public static PGPPrivateKey findPrivateKey(PGPSecretKey pgpSecKey, char[] pass) throws PGPException {
        if (pgpSecKey == null)
            return null;
        PBESecretKeyDecryptor decryptor = (new BcPBESecretKeyDecryptorBuilder((PGPDigestCalculatorProvider)new BcPGPDigestCalculatorProvider())).build(pass);
        return pgpSecKey.extractPrivateKey(decryptor);
    }

    public static void decryptFile(InputStream in, OutputStream out, InputStream keyIn, char[] passwd) throws Exception {
        PGPEncryptedDataList enc;
        Security.addProvider((Provider)new BouncyCastleProvider());
        in = org.bouncycastle.openpgp.PGPUtil.getDecoderStream(in);
        BcPGPObjectFactory pgpF = new BcPGPObjectFactory(in);
        Object o = pgpF.nextObject();
        if (o instanceof PGPEncryptedDataList) {
            enc = (PGPEncryptedDataList)o;
        } else {
            enc = (PGPEncryptedDataList)pgpF.nextObject();
        }
        Iterator<PGPPublicKeyEncryptedData> it = enc.getEncryptedDataObjects();
        PGPPrivateKey sKey = null;
        PGPPublicKeyEncryptedData pbe = null;
        while (sKey == null && it.hasNext()) {
            pbe = it.next();
            sKey = findPrivateKey(keyIn, pbe.getKeyID(), passwd);
        }
        if (sKey == null)
            throw new IllegalArgumentException("Secret key for message not found.");
        InputStream clear = pbe.getDataStream((PublicKeyDataDecryptorFactory)new BcPublicKeyDataDecryptorFactory(sKey));
        BcPGPObjectFactory plainFact = new BcPGPObjectFactory(clear);
        Object message = plainFact.nextObject();
        if (message instanceof PGPCompressedData) {
            PGPCompressedData cData = (PGPCompressedData)message;
            BcPGPObjectFactory pgpFact = new BcPGPObjectFactory(cData.getDataStream());
            message = pgpFact.nextObject();
        }
        if (message instanceof PGPLiteralData) {
            PGPLiteralData ld = (PGPLiteralData)message;
            InputStream unc = ld.getInputStream();
            int ch;
            while ((ch = unc.read()) >= 0)
                out.write(ch);
        } else {
            if (message instanceof PGPOnePassSignatureList)
                throw new PGPException("Encrypted message contains a signed message - not literal data.");
            throw new PGPException("Message is not a simple encrypted file - type unknown.");
        }
        if (pbe.isIntegrityProtected() &&
                !pbe.verify())
            throw new PGPException("Message failed integrity check");
    }

    private static void pipeFileContents(InputStream var3, OutputStream var1, int var2) throws IOException {
        byte[] var4 = new byte[var2];
        int var5;
        while ((var5 = var3.read(var4)) > 0)
            var1.write(var4, 0, var5);
        var1.close();
        var3.close();
    }

    public static void encryptFile(OutputStream out, InputStream fileName, PGPPublicKey encKey, boolean armor, boolean withIntegrityCheck) throws IOException, NoSuchProviderException, PGPException {
        ArmoredOutputStream armoredOutputStream = null;
        Security.addProvider(new BouncyCastleProvider());
        if (armor)
            armoredOutputStream = new ArmoredOutputStream(out);
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        PGPCompressedDataGenerator comData = new PGPCompressedDataGenerator(1);
        PGPLiteralDataGenerator var3 = new PGPLiteralDataGenerator();
        OutputStream var4 = var3.open(comData.open(bOut), 'b', (new Date()).toString(), new Date(), new byte[65536]);
        pipeFileContents(fileName, var4, 4096);
        comData.close();
        BcPGPDataEncryptorBuilder dataEncryptor = new BcPGPDataEncryptorBuilder(2);
        dataEncryptor.setWithIntegrityPacket(withIntegrityCheck);
        dataEncryptor.setSecureRandom(new SecureRandom());
        PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(dataEncryptor);
        encryptedDataGenerator.addMethod(new BcPublicKeyKeyEncryptionMethodGenerator(encKey));
        byte[] bytes = bOut.toByteArray();
        OutputStream cOut = encryptedDataGenerator.open(armoredOutputStream, bytes.length);
        cOut.write(bytes);
        cOut.close();
        armoredOutputStream.close();
    }

    public static boolean isForEncryption(PGPPublicKey key) {
        if (key.getAlgorithm() == 3 || key
                .getAlgorithm() == 17 || key
                .getAlgorithm() == 18 || key
                .getAlgorithm() == 19)
            return false;
        return hasKeyFlags(key, 12);
    }

    private static boolean hasKeyFlags(PGPPublicKey encKey, int keyUsage) {
        if (encKey.isMasterKey()) {
            for (int i = 0; i != MASTER_KEY_CERTIFICATION_TYPES.length; i++) {
                for (Iterator<PGPSignature> eIt = encKey.getSignaturesOfType(MASTER_KEY_CERTIFICATION_TYPES[i]); eIt.hasNext(); ) {
                    PGPSignature sig = eIt.next();
                    if (!isMatchingUsage(sig, keyUsage))
                        return false;
                }
            }
        } else {
            for (Iterator<PGPSignature> eIt = encKey.getSignaturesOfType(24); eIt.hasNext(); ) {
                PGPSignature sig = eIt.next();
                if (!isMatchingUsage(sig, keyUsage))
                    return false;
            }
        }
        return true;
    }

    private static boolean isMatchingUsage(PGPSignature sig, int keyUsage) {
        if (sig.hasSubpackets()) {
            PGPSignatureSubpacketVector sv = sig.getHashedSubPackets();
            if (sv.hasSubpacket(27))
                return sv.getKeyFlags() != 0;
        }
        return true;
    }
}