import java.io.Serializable;

public class Message implements Serializable {
    private String senderId;
    private String message;
    private long timestamp;

    public Message(String senderId, String message, long timestamp) {
        this.senderId = senderId;
        this.message = message;
        this.timestamp = timestamp;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "[" + senderId + "] " + message + " (" + timestamp + ")";
    }
}
