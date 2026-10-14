package io.turbolytics.turbostats.connect.sign;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.EdECPrivateKeySpec;
import java.security.spec.NamedParameterSpec;
import java.util.Arrays;
import java.util.Base64;
import java.util.HexFormat;

/**
 * The private half of an Ed25519 key, as the sfc_ string an operator holds.
 * The public key and the key id derive from it, as in the Go reference.
 */
public final class Credential {
    public static final String PREFIX = "sfc_";
    private static final String PUBLIC_PREFIX = "sfp_";
    private static final int SEED_SIZE = 32;
    private static final byte[] CHECK_MESSAGE = "turbostats key check".getBytes(StandardCharsets.UTF_8);

    private final PrivateKey privateKey;
    private final byte[] publicKey;
    private final String keyId;
    private final byte[] configHashKey;

    private Credential(PrivateKey privateKey, byte[] publicKey, byte[] configHashKey) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.keyId = keyIdOf(publicKey);
        this.configHashKey = configHashKey;
    }

    /** Messages never contain the input: it is a secret. */
    public static Credential parse(String s) {
        if (s == null) {
            throw new IllegalArgumentException("a credential is required");
        }
        if (s.startsWith(PUBLIC_PREFIX)) {
            throw new IllegalArgumentException("that is the public key; turbostats.key takes the sfc_ credential");
        }
        if (!s.startsWith(PREFIX)) {
            throw new IllegalArgumentException("a credential starts with " + PREFIX);
        }
        byte[] seed;
        try {
            seed = Base64.getUrlDecoder().decode(s.substring(PREFIX.length()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("a credential's seed is unpadded base64url");
        }
        if (seed.length != SEED_SIZE) {
            throw new IllegalArgumentException("a credential's seed is " + seed.length + " bytes, not 32");
        }
        KeyPair generated;
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("Ed25519");
            g.initialize(NamedParameterSpec.ED25519, new SeedRandom(seed));
            generated = g.generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("this JVM cannot make Ed25519 keys: " + e.getClass().getSimpleName());
        }
        return fromSeed(seed, generated);
    }

    /**
     * Signs with a private key built from the seed itself, which every
     * provider must honour. Only the public key comes from the generator,
     * and a provider's generator may draw other bytes than the seed, so the
     * pair must sign and verify before it is used. A pair that does not
     * would get every report refused.
     */
    static Credential fromSeed(byte[] seed, KeyPair generated) {
        try {
            PrivateKey privateKey = KeyFactory.getInstance("Ed25519")
                    .generatePrivate(new EdECPrivateKeySpec(NamedParameterSpec.ED25519, seed));
            Signature s = Signature.getInstance("Ed25519");
            s.initSign(privateKey);
            s.update(CHECK_MESSAGE);
            byte[] sig = s.sign();
            s.initVerify(generated.getPublic());
            s.update(CHECK_MESSAGE);
            if (!s.verify(sig)) {
                throw new IllegalArgumentException("the JVM's crypto provider " + s.getProvider().getName()
                        + " derived a different key from the credential; reports signed with it would be refused");
            }
            byte[] encoded = generated.getPublic().getEncoded();
            // An Ed25519 X.509 encoding is a fixed 12-byte prefix and the
            // 32-byte raw key.
            byte[] raw = Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length);
            return new Credential(privateKey, raw, configHashKeyOf(seed));
        } catch (GeneralSecurityException e) {
            throw new IllegalArgumentException("this JVM cannot make Ed25519 keys: " + e.getClass().getSimpleName());
        }
    }

    public String keyId() {
        return keyId;
    }

    public byte[] publicKey() {
        return publicKey.clone();
    }

    /**
     * The key the config hash is keyed with: derived from the seed, so it is
     * stable for one install and different across installs, and never the
     * seed itself. A hash keyed this way cannot be checked against a guessed
     * password by anyone without the credential.
     */
    public byte[] configHashKey() {
        return configHashKey.clone();
    }

    public byte[] sign(byte[] message) {
        try {
            Signature s = Signature.getInstance("Ed25519");
            s.initSign(privateKey);
            s.update(message);
            return s.sign();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("signing failed: " + e.getClass().getSimpleName());
        }
    }

    @Override
    public String toString() {
        // A credential logged by accident shows its key id, never its seed.
        return "Credential(" + keyId + ")";
    }

    private static byte[] configHashKeyOf(byte[] seed) {
        try {
            MessageDigest d = MessageDigest.getInstance("SHA-256");
            d.update("turbostats config hash v1".getBytes(StandardCharsets.UTF_8));
            d.update(seed);
            return d.digest();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /** 16 hex characters of the public key's SHA-256, as the receiver files it. */
    static String keyIdOf(byte[] publicKey) {
        try {
            byte[] sum = MessageDigest.getInstance("SHA-256").digest(publicKey);
            return HexFormat.of().formatHex(sum).substring(0, 16);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Hands the key generator the seed instead of random bytes. The JDK has
     * no API that derives an Ed25519 public key from a seed. OpenJDK's
     * generator draws exactly the 32 private-key bytes from its random
     * source; fromSeed catches a provider that does not.
     */
    private static final class SeedRandom extends SecureRandom {
        private static final long serialVersionUID = 1L;
        private final byte[] seed;

        SeedRandom(byte[] seed) {
            this.seed = seed.clone();
        }

        @Override
        public void nextBytes(byte[] bytes) {
            System.arraycopy(seed, 0, bytes, 0, Math.min(seed.length, bytes.length));
        }
    }
}
