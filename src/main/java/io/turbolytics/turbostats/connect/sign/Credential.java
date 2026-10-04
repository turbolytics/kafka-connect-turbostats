package io.turbolytics.turbostats.connect.sign;

import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.Signature;
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

    private final PrivateKey privateKey;
    private final byte[] publicKey;
    private final String keyId;

    private Credential(PrivateKey privateKey, byte[] publicKey) {
        this.privateKey = privateKey;
        this.publicKey = publicKey;
        this.keyId = keyIdOf(publicKey);
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
        try {
            KeyPairGenerator g = KeyPairGenerator.getInstance("Ed25519");
            g.initialize(NamedParameterSpec.ED25519, new SeedRandom(seed));
            KeyPair kp = g.generateKeyPair();
            byte[] encoded = kp.getPublic().getEncoded();
            // An Ed25519 X.509 encoding is a fixed 12-byte prefix and the
            // 32-byte raw key.
            byte[] raw = Arrays.copyOfRange(encoded, encoded.length - 32, encoded.length);
            return new Credential(kp.getPrivate(), raw);
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
     * no API that derives an Ed25519 public key from a seed; the generator
     * draws exactly the 32 private-key bytes from its random source.
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
