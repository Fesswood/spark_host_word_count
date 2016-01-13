package info.goodline.model;


public class RdrRaw {
    public String dstHost;
    public String dstParam;
    public String serverIp;
    public String userId;
    public String userIp;
    public String info;
    public String date;

    private RdrRaw(String[] c) {
        this.serverIp = checkNull(c[8]);
        this.userId = checkNull(c[3]);
        this.userIp = checkNull(c[12]);
        this.dstHost = checkNull(c[10]);
        this.dstParam = checkNull(c[11]);
        this.date = checkNull(c[0]);
    }

    public static RdrRaw getInstance(String string) {
        String[] c = string.split(",");
        if (c == null || c.length < 12) {
            return null;
        }
        return new RdrRaw(c);
    }

    public String toStringLine() {
        return String.format("%s,%s,%s,%s,%s,%s"
                , date, serverIp, userId, userIp, dstHost, dstParam);
    }

    @Override
    public String toString() {
    /*    return "serverIp = [" + serverIp + "], userIp = [" + userIp + "], info = [" + info + "]"
                + ", hosts= [" + dstHost + "], params = [" + dstParam + "]";*/
        return dstHost;
    }

    private String checkNull(String src) {
        return src == null ? "" : src;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RdrRaw rdrRaw = (RdrRaw) o;
        return !(dstHost != null ? !dstHost.equals(rdrRaw.dstHost) : rdrRaw.dstHost != null);

    }

    @Override
    public int hashCode() {
        int result = dstHost != null ? dstHost.hashCode() : 0;
        return result;
    }
}

