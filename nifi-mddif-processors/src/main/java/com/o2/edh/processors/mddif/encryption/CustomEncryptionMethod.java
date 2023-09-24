package com.o2.edh.processors.mddif.encryption;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public enum CustomEncryptionMethod {
    PGP("PGP", "BC", false, false),
    PGP_ASCII_ARMOR("PGP-ASCII-ARMOR", "BC", false, false);


    private final String algorithm;
    private final String provider;
    private final boolean unlimitedStrength;
    private final boolean compatibleWithStrongKDFs;

    CustomEncryptionMethod(String algorithm, String provider, boolean unlimitedStrength, boolean compatibleWithStrongKDFs) {
        this.algorithm = algorithm;
        this.provider = provider;
        this.unlimitedStrength = unlimitedStrength;
        this.compatibleWithStrongKDFs = compatibleWithStrongKDFs;
    }

    public String getProvider() {
        return this.provider;
    }

    public String getAlgorithm() {
        return this.algorithm;
    }

    public boolean isKeyedCipher() {
        return !this.algorithm.startsWith("PBE") && !this.algorithm.startsWith("PGP");
    }

    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this);
        ToStringBuilder.setDefaultStyle(ToStringStyle.SHORT_PREFIX_STYLE);
        builder.append("Algorithm name", this.algorithm);
        builder.append("Requires unlimited strength JCE policy", this.unlimitedStrength);
        builder.append("Algorithm Provider", this.provider);
        builder.append("Compatible with strong KDFs", this.compatibleWithStrongKDFs);
        builder.append("Keyed cipher", this.isKeyedCipher());
        return builder.toString();
    }

}
