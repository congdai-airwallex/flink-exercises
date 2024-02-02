package org.example.model;

public class Event {
    public Long timestamp;
    public String type;
    public String message;
    public Event(Long timestamp, String type, String message) {
        this.timestamp = timestamp;
        this.type = type;
        this.message = message;
    }
    static public Event parseFromString(String value) {
        String[] parts = value.split("\\s+");
        if(parts.length!= 3) {
            return null;
        }
        try {
            long timestamp = Long.parseLong(parts[0]);
            String type = parts[1];
            String message = parts[2];
            return new Event(timestamp, type, message);
        } catch (NumberFormatException e) {
            //e.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        return "Event{" +
                "timestamp=" + timestamp +
                ", type='" + type + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
