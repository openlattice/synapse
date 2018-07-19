package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.List;
import java.util.Map;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

@JsonIgnoreProperties( value = { TRANSFORM } )

public class ConditionalPrefixSubstringTransform extends Transformation<String> {
    private final String prefix;
    private final int substringlocation;

    @JsonCreator
    public ConditionalPrefixSubstringTransform(
            @JsonProperty( Constants.PREFIX ) String prefix,
            @JsonProperty( Constants.LOC ) int substringlocation ) {
        this.prefix = prefix;
        this.substringlocation = substringlocation;
    }

    @JsonProperty( Constants.PREFIX )
    public String getPrefix() {
        return prefix;
    }

    @JsonProperty( Constants.LOC )
    public int getSubstringLocation() {
        return substringlocation;
    }

    @Override
    public Object apply( String o ) {
        if ( o.startsWith(prefix) ) {
            return o.substring( substringlocation );
        }
        return o;
    }
}
