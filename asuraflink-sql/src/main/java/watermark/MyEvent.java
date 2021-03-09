package watermark;

public class MyEvent {
    public boolean watermarkMarker;
    public long watermarkTimestamp;
    public boolean hasWatermarkMarker() {
        return watermarkMarker;
    }
    public long getWatermarkTimestamp() {
        return watermarkTimestamp;
    }
}
