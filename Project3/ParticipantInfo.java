public class ParticipantInfo {
    private String id;
    private String ip;
    private int port;
    private String status;
    private long lastDelivered = 0; 

    public ParticipantInfo(String id, String ip, int port, String status) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.status = status;
    }

    public String getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getStatus() {
        return status;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getLastDelivered() { 
        return lastDelivered; 
    }
    
    public void setLastDelivered(long ts) { 
        this.lastDelivered = ts; 
    }
}