package transforms;

import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.transformations.Transformation;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseBoolTransform extends Transformation<String> {

    private static final Logger logger = LoggerFactory
            .getLogger(Shuttle.class);

    /**
     * Represents a transformation to parse booleans from a string.
     */
    public ParseBoolTransform() {
    }

    private boolean convertToBoolean(String value) {
        boolean returnValue = false;
        if ("1".equalsIgnoreCase(value) || "yes".equalsIgnoreCase(value) ||
                "true".equalsIgnoreCase(value) || "on".equalsIgnoreCase(value)) {
            returnValue = true;
        }
        return returnValue;
    }

    @Override
    public Object applyValue(String o) {
        if (StringUtils.isNotBlank(o)) {
            try {
                return convertToBoolean(o);
            } catch (IllegalArgumentException e) {
                logger.error("Unable to parse boolean from value {}", o);
            }
        }
        return null;
    }

}
