package info.goodline.model;

/**
 * Created by fesswood on 29.10.15.
 */
public class RdrParser {

    public static RdrRaw parseRdr(String string) {
        RdrRaw result = null;
        if (string.contains("TIME_STAMP")) {
            return null;
        }
        result = RdrRaw.getInstance(string);
        return result;
    }
}
