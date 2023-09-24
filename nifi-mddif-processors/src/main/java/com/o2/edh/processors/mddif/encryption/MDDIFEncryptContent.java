package com.o2.edh.processors.mddif.encryption;

import com.o2.edh.processors.mddif.util.*;
import com.o2.edh.processors.mddif.encryption.OpenPGPKeyBasedEncryptor;
import com.o2.edh.processors.mddif.encryption.OpenPGPPasswordBasedEncryptor;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.*;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.*;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.bc.*;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.Normalizer;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.bouncycastle.bcpg.PublicKeyAlgorithmTags.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"encryption", "decryption", "password", "JCE", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Encrypts or Decrypts a FlowFile using either symmetric encryption with a password and randomly generated salt, or asymmetric encryption using a public and secret key.")
@SystemResourceConsideration(resource = SystemResource.CPU)
public class MDDIFEncryptContent extends AbstractProcessor {

    public static final String ENCRYPT_MODE = "Encrypt";
    public static final String DECRYPT_MODE = "Decrypt";

    private boolean asciiArmored = true;
    private boolean integrityCheck = true;

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encrypted or decrypted")
            .required(true)
            .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
            .defaultValue(DECRYPT_MODE)
            .build();
    public static final PropertyDescriptor ENCRYPTION_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Encryption Algorithm")
            .description("The Encryption Algorithm to use")
            .required(true)
            .allowableValues(buildEncryptionMethodAllowableValues())
            .defaultValue(CustomEncryptionMethod.PGP.name())
            .build();
    public static final PropertyDescriptor SALT = new PropertyDescriptor.Builder()
            .name("SALT")
            .description("The salt to get password for decrypting the data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor PUBLIC_KEYRING = new PropertyDescriptor.Builder()
            .name("public-keyring-file")
            .displayName("Public Keyring File")
            .description("In a PGP encrypt mode, this keyring contains the public key of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor PUBLIC_KEY_USERID = new PropertyDescriptor.Builder()
            .name("public-key-user-id")
            .displayName("Public Key User Id")
            .description("In a PGP encrypt mode, this user id of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor PRIVATE_KEYRING = new PropertyDescriptor.Builder()
            .name("private-keyring-file")
            .displayName("Private Keyring File")
            .description("In a PGP decrypt mode, this keyring contains the private key of the recipient")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor PRIVATE_KEYRING_PASSPHRASE = new PropertyDescriptor.Builder()
            .name("private-keyring-passphrase")
            .displayName("Private Keyring Passphrase")
            .description("In a PGP decrypt mode, this is the private keyring passphrase")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PGP_SYMMETRIC_ENCRYPTION_CIPHER = new PropertyDescriptor.Builder()
            .name("pgp-symmetric-cipher")
            .displayName("PGP Symmetric Cipher")
            .description("When using PGP encryption, this is the symmetric cipher to be used. This property is ignored if "
                    + "Encryption Algorithm is not PGP or PGP-ASCII-ARMOR\nNote that the provided cipher is only used during"
                    + "the encryption phase, while it is inferred from the cipher text in the decryption phase")
            .required(false)
            .allowableValues(buildPGPSymmetricCipherAllowableValues())
            .defaultValue(String.valueOf(PGPEncryptedData.AES_128))
            .build();
    public static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool")
            .description("Specifies the JDBC Connection Pool to use in order to convert the JSON message to a SQL statement. "
                    + "The Connection Pool is necessary in order to determine the appropriate database column types.")
            .identifiesControllerService(DBCPService.class)
            .required(true)    //false just for test purpose changed
            .build();
    public static final PropertyDescriptor CONFIGURATION_TABLE = new PropertyDescriptor.Builder()
            .name("config-table")
            .displayName("Configuration Table")
            .description("The configuration table's name to load the configuration from.")
            .required(true)    //false just for test purpose changed
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully encrypted or decrypted will be routed to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be encrypted or decrypted will be routed to failure")
            .build();
    public static final Relationship LOG_RELATIONSHIP = new Relationship.Builder()
            .name("log")
            .description("Log relationship")
            .build();

    private List<PropertyDescriptor> properties;

    private Set<Relationship> relationships;

    static {
        // add BouncyCastle encryption providers
        Security.addProvider(new BouncyCastleProvider());
    }

    private static AllowableValue[] buildEncryptionMethodAllowableValues() {
        final CustomEncryptionMethod[] encryptionMethods = CustomEncryptionMethod.values();
        List<AllowableValue> allowableValues = new ArrayList<>(encryptionMethods.length);
        for (CustomEncryptionMethod em : encryptionMethods) {
            allowableValues.add(new AllowableValue(em.name(), em.name(), em.toString()));
        }
        return allowableValues.toArray(new AllowableValue[0]);
    }

    private static AllowableValue[] buildPGPSymmetricCipherAllowableValues() {
        // Allowed values are inferred from SymmetricKeyAlgorithmTags. Note that NULL and SAFER ciphe
        //
        // r are not supported and therefore not listed
        return new AllowableValue[] {
                new AllowableValue("1", "IDEA"),
                new AllowableValue("2", "TRIPLE_DES"),
                new AllowableValue("3", "CAST5"),
                new AllowableValue("4", "BLOWFISH"),
                new AllowableValue("6", "DES"),
                new AllowableValue("7", "AES_128"),
                new AllowableValue("8", "AES_192"),
                new AllowableValue("9", "AES_256"),
                new AllowableValue("10", "TWOFISH"),
                new AllowableValue("11", "CAMELLIA_128"),
                new AllowableValue("12", "CAMELLIA_192"),
                new AllowableValue("13", "CAMELLIA_256") };
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(ENCRYPTION_ALGORITHM);
        properties.add(SALT);
        properties.add(PUBLIC_KEYRING);
        properties.add(PUBLIC_KEY_USERID);
        properties.add(PRIVATE_KEYRING);
        properties.add(PRIVATE_KEYRING_PASSPHRASE);
        properties.add(PGP_SYMMETRIC_ENCRYPTION_CIPHER);
        properties.add(CONNECTION_POOL);
        properties.add(CONFIGURATION_TABLE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(LOG_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    public static boolean isPGPArmoredAlgorithm(final String algorithm) {
        return isPGPAlgorithm(algorithm) && algorithm.endsWith("ASCII-ARMOR");
    }

    public static boolean isPGPAlgorithm(final String algorithm) {
        return algorithm.startsWith("PGP");
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile[] flowFile = {session.get()};

        if (flowFile[0] == null) {
            return;
        }

        final String fileName = flowFile[0].getAttribute("file_name");
        final String confId = flowFile[0].getAttribute("conf_id");
        final String fileUuid = flowFile[0].getAttribute("file_uuid");
        final String salt = context.getProperty(SALT).evaluateAttributeExpressions(flowFile[0]).getValue();
        final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);
        final String config_table = context.getProperty(CONFIGURATION_TABLE).evaluateAttributeExpressions(flowFile[0]).getValue();

        Logger logger = new Logger(session, getLogger(), LOG_RELATIONSHIP);  // [LOGGING]: Init Log
        logger.generateLog(new Log(LogLevel.DEBUG,null,null,null,"123003",
                "Logger initiated"));

        String password = null;
        final String[] message = new String[1];
        if(!encrypt) {
            final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
            final Connection con = dbcpService.getConnection();
            try {
                final Statement st = con.createStatement();
                String query = "select cast(aes_decrypt(encryption_key_password, '" + salt + "') as char) password " +
                        "from " + config_table + " where conf_id =" + flowFile[0].getAttribute("conf_id");
                final ResultSet rs = st.executeQuery(query);
                if (rs.next()) {
                    password = rs.getString(1);
                } else {
                    message[0] = "Failed to get passphrase as no record found in configuration_table";
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "323008", message[0]));
                    session.rollback();
                    context.yield();
                    return;
                }

            } catch (SQLException e1) {
                e1.printStackTrace();
                message[0] = "SQLError while retrieving passphrase because - " + e1.getClass().getName() + " reason: " + e1.getMessage();
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "323008", message[0]));
                session.rollback();
                context.yield();
                return;
            } finally {
                try {
                    con.close();
                } catch (SQLException e) {
                    message[0] = "Failed to close SQL connection - " + e.getClass().getName() + " reason: " + e.getMessage();
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "323009", message[0]));
                    session.rollback();
                    context.yield();
                }
            }
        }

        int cryptAlgo = 0;
        FileInputStream keyTemp = null;
        try {
            if(encrypt) {
                keyTemp = new FileInputStream(context.getProperty(PUBLIC_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue());

                PGPPublicKey PubK = MDDIFPGPUtils.readPublicKey(keyTemp);
                cryptAlgo = PubK.getAlgorithm();
            }

            else {
                keyTemp = new FileInputStream(context.getProperty(PRIVATE_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue());

                PGPSecretKey secKey = readSecretKey(keyTemp);
                final char[] keyringPassphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                PGPPrivateKey privKey = MDDIFPGPUtils.findPrivateKey(secKey, keyringPassphrase);

                cryptAlgo = privKey.getPublicKeyPacket().getAlgorithm();
            }
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323016","Encryption Key cannot be found"),flowFile[0], REL_FAILURE);
            return;
        }
        catch (PGPException e1) {
            e1.printStackTrace();
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323017","Invalid Key for decryption"),flowFile[0], REL_FAILURE);
            return;
        }
        catch(Exception e2) {
            e2.printStackTrace();
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323018","Encryption Key Password is null"),flowFile[0], REL_FAILURE);
            return;
        }
        finally {
            try {
                if(keyTemp != null)
                    keyTemp.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323019","IOException while closing security key"),flowFile[0], REL_FAILURE);
                return;
            }
        }


        if (cryptAlgo == RSA_GENERAL || cryptAlgo == RSA_ENCRYPT || cryptAlgo == RSA_SIGN) {

            //logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName, "123003", "Logger initiated"));

            final String method = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
            final CustomEncryptionMethod encryptionMethod = CustomEncryptionMethod.valueOf(method);
            final String providerName = encryptionMethod.getProvider();
            final String algorithm = encryptionMethod.getAlgorithm();
            final Integer pgpCipher = context.getProperty(PGP_SYMMETRIC_ENCRYPTION_CIPHER).asInteger();

            if(!encrypt && password==null){
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323014","Encryption Key Password is null"),flowFile[0], REL_FAILURE);
                return;
            } else if(!encrypt && password.equalsIgnoreCase("")){
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323015","Encryption Key Password is ''"),flowFile[0], REL_FAILURE);
                return;
            }

            Encryptor encryptor = null;
            StreamCallback callback;

            try {
                if (isPGPAlgorithm(algorithm)) {
                    final String filename = flowFile[0].getAttribute(CoreAttributes.FILENAME.key());
                    final String publicKeyring = context.getProperty(PUBLIC_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue();
                    final String privateKeyring = context.getProperty(PRIVATE_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue();
                    if (encrypt && publicKeyring != null) {
                        final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).evaluateAttributeExpressions(flowFile[0]).getValue();
                        message[0] = " PGP Encryption with public keyring for file :: " + filename;
                        logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, filename,"223004",message[0]));
                        encryptor = (Encryptor) new OpenPGPKeyBasedEncryptor(algorithm, pgpCipher, providerName, publicKeyring, publicUserId, null, filename);
                    } else if (!encrypt && privateKeyring != null) {
                        final char[] keyringPassphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                        message[0] = " PGP Decryption with private key for file :: " + filename;
                        logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, filename,"223005",message[0]));
                        encryptor = (Encryptor) new OpenPGPKeyBasedEncryptor(algorithm, pgpCipher, providerName, privateKeyring, null, keyringPassphrase, filename);
                    } else {
                        final char[] passphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                        message[0] = " PGP Decryption with passPhrase for file :: "  + filename;
                        logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, filename,"223006",message[0]));
                        encryptor = (Encryptor) new OpenPGPPasswordBasedEncryptor(algorithm, pgpCipher, providerName, passphrase, filename);
                    }
                }

                if (encrypt) {
                    callback = encryptor.getEncryptionCallback();
                } else {
                    callback = encryptor.getDecryptionCallback();
                }

            } catch (final Exception e) {
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323011",
                                "Failed to initialize " + ((encrypt)?"Encryption":"Decryption") + " algorithm because " + e.getMessage()),
                        flowFile[0], REL_FAILURE);
                session.rollback();
                context.yield();
                return;
            }

            try {
                final StopWatch stopWatch = new StopWatch(true);
                flowFile[0] = session.write(flowFile[0], callback);
                logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"223007","Successfully " + ((encrypt)?"Encrypted":"Decrypted") ));
                session.getProvenanceReporter().modifyContent(flowFile[0], stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(flowFile[0], REL_SUCCESS);
            } catch (final ProcessException e) {
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323012",
                        "Cannot " + ((encrypt)?"encrypt ":"decrypt ") + "due to " + e.getMessage()), flowFile[0], REL_FAILURE);
            }
        }
        else if (cryptAlgo == DSA || cryptAlgo == ELGAMAL_ENCRYPT) {
            try {
                final boolean[] error = { false };
                FlowFile parsed = session.create(flowFile[0]);
                String finalPassword = (password == null) ? "" : password;

                parsed = session.write(parsed, parsedOut -> session.read(flowFile[0], originalIn -> {
                    String logMessage = "";
                    try {
                        if (encrypt) {
                            FileInputStream keyIn = new FileInputStream(context.getProperty(PUBLIC_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue());
                            message[0] = " PGP Encryption with public keyring for file :: " + fileName;
                            logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"223004", message[0]));
                            MDDIFPGPUtils.encryptFile(parsedOut, originalIn, MDDIFPGPUtils.readPublicKey(keyIn), MDDIFEncryptContent.this.asciiArmored, MDDIFEncryptContent.this.integrityCheck);
                            keyIn.close();

                        } else {
                            FileInputStream keyIn = new FileInputStream(context.getProperty(PRIVATE_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue());
                            message[0] = " PGP Decryption with private key for file :: " + fileName;
                            logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"223005",message[0]));
                            MDDIFPGPUtils.decryptFile(originalIn, parsedOut, keyIn, finalPassword.toCharArray());
                            keyIn.close();
                        }
                        logMessage = "Successfully " + (encrypt ? "en" : "de") + "crypted FlowFile, for source with conf-id : " + confId;
                        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName,"123004",logMessage));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        error[0] = true;
                    }
                }));
                if (!error[0]) {
                    session.remove(flowFile[0]);
                    logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName, "223006",
                            "Flowfile is successfully " + (encrypt?"encrypted. ":"decrypted. ") + "Transferring to success."));
                    session.transfer(parsed, REL_SUCCESS);
                } else {
                    session.transfer(flowFile[0], REL_FAILURE);
                    session.remove(parsed);
                }
            } catch (Exception ex) {
                logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,
                        "323007","Exception occurred during: " + (encrypt?"encryption ":"decryption ") + ex.getClass().getName()
                                + " reason : " + ex.getMessage()),
                        flowFile[0], REL_FAILURE);
            }
        }

        /*try {
            final boolean[] error = { false };
            FlowFile parsed = session.create(flowFile[0]);
            String finalPassword = (password == null) ? "" : password;
            final String filename = flowFile[0].getAttribute(CoreAttributes.FILENAME.key());
            parsed = session.write(parsed, parsedOut -> session.read(flowFile[0], originalIn -> {
                Encryptor encryptor = null;
                StreamCallback callback = null;
                try {
                    String logMessage = "";

                    final String method = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
                    final CustomEncryptionMethod encryptionMethod = CustomEncryptionMethod.valueOf(method);
                    final String providerName = encryptionMethod.getProvider();
                    final String algorithm = encryptionMethod.getAlgorithm();
                    final String publicKeyring = context.getProperty(PUBLIC_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue();
                    final String privateKeyring = context.getProperty(PRIVATE_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue();
                    final Integer pgpCipher = context.getProperty(PGP_SYMMETRIC_ENCRYPTION_CIPHER).asInteger();
                    final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).evaluateAttributeExpressions(flowFile[0]).getValue();

                    if (encrypt) {
                        FileInputStream keyIn = new FileInputStream(context.getProperty(PUBLIC_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue());

                        PGPPublicKey PubK = MDDIFPGPUtils.readPublicKey(keyIn);
                        int encAlgo = PubK.getAlgorithm();

                        if (encAlgo == RSA_GENERAL || encAlgo == RSA_ENCRYPT)
                        {
                            message[0] = " PGP Encryption with public keyring for file :: " + filename;
                            logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"223004", message[0]));
                            encryptor = (Encryptor) new OpenPGPKeyBasedEncryptor(algorithm, pgpCipher, providerName, publicKeyring, publicUserId, null, filename);

                            callback = encryptor.getEncryptionCallback();
                        }
                        else if (encAlgo == DSA) {
                            MDDIFPGPUtils.encryptFile(parsedOut, originalIn, MDDIFPGPUtils.readPublicKey(keyIn), MDDIFEncryptContent.this.asciiArmored, MDDIFEncryptContent.this.integrityCheck);
                            keyIn.close();
                        }

                        logMessage = "Successfully encrypted FlowFile, for source with conf-id : " + confId;
                        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName,"123004",logMessage));

                    } else {
                        FileInputStream keyIn = new FileInputStream(context.getProperty(PRIVATE_KEYRING).evaluateAttributeExpressions(flowFile[0]).getValue());

                        PGPSecretKey secKey = readSecretKey(keyIn);
                        final char[] keyringPassphrase = Normalizer.normalize(finalPassword, Normalizer.Form.NFC).toCharArray();
                        PGPPrivateKey privKey = MDDIFPGPUtils.findPrivateKey(secKey, keyringPassphrase);

                        int decAlgo = privKey.getPublicKeyPacket().getAlgorithm();

                        if (decAlgo == DSA) {
                            MDDIFPGPUtils.decryptFile(originalIn, parsedOut, keyIn, finalPassword.toCharArray());
                            keyIn.close();
                        }
                        else if(decAlgo == RSA_ENCRYPT || decAlgo == RSA_GENERAL) {
                            message[0] = " PGP Decryption with private key for file :: " + filename;
                            logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"223005",message[0]));
                            encryptor = (Encryptor) new OpenPGPKeyBasedEncryptor(algorithm, pgpCipher, providerName, privateKeyring, null, keyringPassphrase, filename);

                            callback = encryptor.getEncryptionCallback();
                        }
                        logMessage = "Successfully decrypted FlowFile, for source with conf-id : " + confId;
                        logger.generateLog(new Log(LogLevel.DEBUG, confId, fileUuid, fileName,"123005",logMessage));
                    }

                    if (encrypt) {
                        callback = encryptor.getEncryptionCallback();
                    } else {
                        callback = encryptor.getDecryptionCallback();
                    }

                    try {
                        final StopWatch stopWatch = new StopWatch(true);
                        flowFile[0] = session.write(flowFile[0], callback);
                        logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName,"223007","successfully " + ((encrypt)?"Encrypted":"Decrypted") ));
                        session.getProvenanceReporter().modifyContent(flowFile[0], stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                        session.transfer(flowFile[0], REL_SUCCESS);
                    } catch (final ProcessException e) {
                        logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName,"323012",
                                "Cannot " + ((encrypt)?"encrypt ":"decrypt ") + "due to " + e.getMessage()), flowFile[0], REL_FAILURE);
                    }

                } catch (Exception ex) {
                    ex.printStackTrace();
                    logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "323007",
                             "Exception occurred while : " + (encrypt?"encryption ":"decryption ")
                                     + ex.getClass().getName() + " reason: " + ex.getMessage()));
                    error[0] = true;
                }finally {
                    originalIn.close();
                    parsedOut.close();
                }
            }));

            if (!error[0]) {
                session.remove(flowFile[0]);
                logger.generateLog(new Log(LogLevel.INFO, confId, fileUuid, fileName, "223006",
                        "Flowfile is successfully " + (encrypt?"encrypted. ":"decrypted. ") + "Transferring to success."));
                session.transfer(parsed, REL_SUCCESS);
            } else {
                session.transfer(flowFile[0], REL_FAILURE);
                session.remove(parsed);
            }
        } catch (Exception ex) {
            logger.generateLog(new Log(LogLevel.ERROR, confId, fileUuid, fileName, "323007",
                            "Exception occurred : " + (encrypt?"encryption ":"decryption ")
                                    + ex.getClass().getName() + " reason : " + ex.getMessage()),
                    flowFile[0], REL_FAILURE);
        }*/
    }

    static PGPSecretKey readSecretKey(InputStream input) throws Exception {
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(input),
                new BcKeyFingerprintCalculator());

        Iterator keyRingIter = pgpSec.getKeyRings();
        while (keyRingIter.hasNext()) {
            PGPSecretKeyRing keyRing = (PGPSecretKeyRing) keyRingIter.next();

            Iterator keyIter = keyRing.getSecretKeys();
            while (keyIter.hasNext()) {
                PGPSecretKey key = (PGPSecretKey) keyIter.next();
                if (key.isSigningKey()) {
                    return key;
                }
            }
        }

        throw new IllegalArgumentException("Can't find signing key in key ring.");
    }

    public interface Encryptor {
        StreamCallback getEncryptionCallback() throws Exception;

        StreamCallback getDecryptionCallback() throws Exception;
    }

}