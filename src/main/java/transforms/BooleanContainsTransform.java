package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.BooleanTransformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BooleanContainsTransform extends BooleanTransformation {
    private final String column;
    private final String string;
    private final Boolean ignoreCase;
    /**
     * Represents a selection of transformations based on whether a column
     * contains a specific value or not.  If either transformsIfTrue or transformsIfFalse are empty,
     * the value of the tested column will be passed on.
     *
     * @param column:            column to test for string
     * @param string:           string to test column against
     * @param ignoreCase:        whether to ignore case in string
     * @param transformsIfTrue:  transformations to do on column value if exists
     * @param transformsIfFalse: transformations to do if does not exist (note ! define columntransform to choose column !)
     */
    @JsonCreator
    public BooleanContainsTransform(
            @JsonProperty(Constants.COLUMN) String column,
            @JsonProperty(Constants.STRING) String string,
            @JsonProperty(Constants.IGNORE_CASE) Optional<Boolean> ignoreCase,
            @JsonProperty(Constants.TRANSFORMS_IF_TRUE) Optional<Transformations> transformsIfTrue,
            @JsonProperty(Constants.TRANSFORMS_IF_FALSE) Optional<Transformations> transformsIfFalse) {
        super(transformsIfTrue, transformsIfFalse);
        this.column = column;
        this.ignoreCase = ignoreCase == null ? false : true;
        if (this.ignoreCase) {
            this.string = string.toLowerCase();
        } else {
            this.string = string;
        }

     }

    @JsonProperty(Constants.COLUMN)
    public String getColumn() {
        return column;
    }

    @Override
    public boolean applyCondition(Map<String, String> row) {
        String o = row.get(column);

        if (StringUtils.isBlank(o)) {
            return false;
        }

        if (this.ignoreCase) {
            o = o.toLowerCase();
        }

        return o.contains(string);

      }
}

