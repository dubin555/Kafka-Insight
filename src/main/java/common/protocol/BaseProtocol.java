package common.protocol;

import com.google.gson.Gson;

/**
 * Created by dubin on 29/09/2017.
 */
public class BaseProtocol {

    public String toString() {
        return new Gson().toJson(this);
    }
}
