package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.List;
import java.util.Map;

public class NestedTransformsTransform extends Transformation<Map<String, Object>> {

    private final TransformValueMapper transformValueMapper;

    /**
     * Represents a series of transformations to be used as an argument (nested within other transformations)
     *
     * @param transforms: transforms
     */
    @JsonCreator
    public NestedTransformsTransform(
            @JsonProperty( Constants.TRANSFORMS ) List<Transformation> transforms
    ) {
        this.transformValueMapper = new TransformValueMapper(transforms);
    }

    @Override
    public Object apply( Map<String, Object> row ) {

        String outstring = this.transformValueMapper.apply( row ).toString();

        return ( outstring );
    }

}

