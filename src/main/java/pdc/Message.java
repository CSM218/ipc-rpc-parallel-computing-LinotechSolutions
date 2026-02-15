package pdc;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public String magic;
    public int version;
    public String type;
    public String messageType;
    public String sender;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.studentId = System.getenv("STUDENT_ID") != null ? System.getenv("STUDENT_ID") : "default";
    }

    /**
     * Convenience: get the messageType (alias for type field)
     */
    public String getMessageType() {
        return messageType != null ? messageType : type;
    }

    /**
     * Set both type and messageType for protocol compliance
     */
    public void setMessageType(String msgType) {
        this.type = msgType;
        this.messageType = msgType;
    }

    /**
     * Serialize the message to a JSON string representation.
     * Used for logging and debugging of protocol messages.
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"magic\":\"").append(magic).append("\",");
        sb.append("\"version\":").append(version).append(",");
        sb.append("\"messageType\":\"").append(getMessageType()).append("\",");
        sb.append("\"studentId\":\"").append(studentId).append("\",");
        sb.append("\"timestamp\":").append(timestamp).append(",");
        sb.append("\"payload\":\"").append(payload != null ? "binary[" + payload.length + "]" : "null").append("\"");
        sb.append("}");
        return sb.toString();
    }

    /**
     * Validate the message against the CSM218 protocol specification.
     * Throws an exception if the message is malformed.
     */
    public void validate() throws IllegalArgumentException {
        if (magic == null || !magic.equals("CSM218")) {
            throw new IllegalArgumentException("Invalid magic: expected CSM218, got " + magic);
        }
        if (version < 1) {
            throw new IllegalArgumentException("Invalid version: " + version);
        }
        if (getMessageType() == null || getMessageType().isEmpty()) {
            throw new IllegalArgumentException("Missing messageType");
        }
    }

    /**
     * Parse a message from a simple string format.
     * Used as a fallback deserialization mechanism.
     */
    public static Message parse(String input) {
        Message msg = new Message();
        msg.magic = "CSM218";
        msg.version = 1;
        msg.type = input;
        msg.messageType = input;
        msg.timestamp = System.currentTimeMillis();
        return msg;
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            // Write magic string (null-safe) - CSM218 protocol
            writeString(dos, magic != null ? magic : "CSM218");
            
            // Write version
            dos.writeInt(version);
            
            // Write type / messageType
            writeString(dos, getMessageType());
            
            // Write sender
            writeString(dos, sender);
            
            // Write studentId
            writeString(dos, studentId);
            
            // Write timestamp
            dos.writeLong(timestamp);
            
            // Write payload (length-prefixed)
            if (payload == null) {
                dos.writeInt(0);
            } else {
                dos.writeInt(payload.length);
                dos.write(payload);
            }
            
            dos.flush();
            byte[] messageData = baos.toByteArray();
            
            // Create frame with length prefix
            ByteArrayOutputStream frameStream = new ByteArrayOutputStream();
            DataOutputStream frameOut = new DataOutputStream(frameStream);
            frameOut.writeInt(messageData.length);
            frameOut.write(messageData);
            frameOut.flush();
            
            return frameStream.toByteArray();
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        try {
            // Outer stream includes length prefix
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Message msg = new Message();

            int frameLength = dis.readInt();
            if (frameLength < 0) {
                throw new IllegalArgumentException("Invalid frame length: " + frameLength);
            }

            // Read exactly frameLength bytes for the message body
            byte[] frame = new byte[frameLength];
            dis.readFully(frame);

            // Now parse the message body
            DataInputStream body = new DataInputStream(new ByteArrayInputStream(frame));

            msg.magic = readString(body);
            msg.version = body.readInt();
            msg.type = readString(body);
            msg.messageType = msg.type;
            msg.sender = readString(body);
            msg.studentId = readString(body);
            msg.timestamp = body.readLong();

            int payloadLen = body.readInt();
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                body.readFully(msg.payload);
            }
            
            return msg;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to unpack message", e);
        }
    }
    
    // Helper: write length-prefixed string
    private static void writeString(DataOutputStream dos, String str) throws IOException {
        if (str == null) {
            dos.writeInt(-1);
        } else {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    }
    
    // Helper: read length-prefixed string
    private static String readString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}