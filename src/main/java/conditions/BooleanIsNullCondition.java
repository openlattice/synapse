package conditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BooleanIsNullCondition extends Condition<Map<String, String>> {
    private final String                                       column;
    private final Boolean                                      reverse;

    /**
     * Represents a transformation to select columns based on non-empty cells.
     * Function goes over columns until a non-zero input is found.
     *
     * @param column:            column to test for pattern
     * @param reverse:           if the condition needs to be reversed (not)
     */
    @JsonCreator
    public BooleanIsNullCondition(
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.REVERSE ) Boolean reverse ) {
        this.column = column;
        this.reverse = reverse == null ? false : reverse ;

    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }

    @JsonProperty( Constants.REVERSE )
    public Boolean getReverse() {
        return reverse;
    }

    @Override
    public Boolean apply( Map<String, String> row ) {
        if ( StringUtils.isBlank( row.get( column ) ) ) {
            return true;
        } else {
            return false;
        }
    }

}

