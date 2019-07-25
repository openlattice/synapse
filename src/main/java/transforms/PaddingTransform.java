package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;

public class PaddingTransform extends Transformation<Map<String, String>> {
    private final String pattern;
    private final int length;
    private final boolean pre;
    private final boolean cutoff;

    /**
     * A transform that pads the input string to a certain length by appending or prepending a repeating pattern
     *
     * @param pattern:            pad will be constructed by repeating this string
     * @param length:             target length of resulting string
     * @param pre:                whether to prepend (as opposed to append)
     * @param cutoff:             whether to trim the string to the target size if it's larger
     *
     */
    @JsonCreator
    public PaddingTransform(
            @JsonProperty( Constants.PATTERN ) String pattern,
            @JsonProperty( Constants.LENGTH ) int length,
            @JsonProperty( Constants.PRE ) Optional<Boolean> pre,
            @JsonProperty( Constants.CUTOFF ) Optional<Boolean> cutoff
            ) {
        this.pattern = pattern;
        this.length = length;
        this.pre = pre.orElse(false);
        this.cutoff = cutoff.orElse(false);
    }

    @JsonProperty( Constants.PATTERN )
    public String getPattern() {
        return pattern;
    }

    @JsonProperty( Constants.LENGTH )
    public int getLength() {
        return length;
    }

    @JsonProperty( Constants.PRE )
    public boolean getPre() {
        return pre;
    }

    @JsonProperty( Constants.CUTOFF )
    public boolean getCutoff() {
        return cutoff;
    }

    private String getTrimmedString( String input ) {
        if ( !cutoff ) {
            return input;
        }

        int startIndex = pre ? input.length() - length : 0;
        return input.substring( startIndex, startIndex + length );
    }

    @Override
    public Object applyValue( String o ) {

        if (length < 0) {
            throw new IllegalStateException( "Negative length given." );
        }

        if (pattern.equals("")) {
            throw new IllegalStateException( "Empty pattern given." );
        }

        if (o.length() > length) {
            return getTrimmedString( o );
        }

        // The input string is small. Iteratively append pattern.

        StringBuilder builder = new StringBuilder(o);
        if (pre) {
            while (length > builder.length()) {
                builder.insert(0,pattern);
            }
            return builder.substring(builder.length() - length);  //trim excess pattern that was added and return.
        }
        else {
            while (length > builder.length()) {
                builder.append(pattern);
            }
            return builder.substring(0, length);  //trim excess pattern that was added and return.
        }
    }

}