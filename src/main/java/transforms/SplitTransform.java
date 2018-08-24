/*
 * Copyright (C) 2018. OpenLattice, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 *
 */

package transforms;

import static java.lang.Integer.parseInt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

public class SplitTransform extends Transformation<String> {

    private final String separator;
    private final String valueElse;
    private final String index;
    private final Integer ifMoreThan;

    /**
     * Represents a transformation to split a string. Example:
     * MiddleName is a SplitTransform with arguments = {sep = " ", index = 1, ifMoreThan = 3}
     * "Joke Christine Durnez" --> "Christine"
     * "Joke Durnez" --> null
     * "Joke" --> null
     *
     *
     * @param separator: separate by what?
     * @param index:     index to grab (starts at 0!), can be 'last'
     * @param valueElse:      string to return if there is no match based on the splitter
     * @param ifMoreThan: if the i'th place should only be returned if there are more than x occurenced
     */
    @JsonCreator
    public SplitTransform(
            @JsonProperty(Constants.SEP) String separator,
            @JsonProperty(Constants.INDEX) String index,
            @JsonProperty(Constants.ELSE) String valueElse,
            @JsonProperty(Constants.IF_MORE_THAN) Integer ifMoreThan
    ) {
        this.separator = separator;
        this.index = index;
        this.ifMoreThan = ifMoreThan;
        this.valueElse = valueElse == null ? "" : valueElse;
    }

    @Override
    public Object apply(String o) {
        if (StringUtils.isBlank(o)) {
            return null;
        }
        String[] strNames = o.trim().split(separator);

        int idx = 0;
        if (index.equals("last")) {
            idx = strNames.length - 1;
        } else {
            idx = parseInt(index);
        }

        if (!(ifMoreThan == null)) {
            if (!(ifMoreThan < strNames.length)) {
                return null;
            }
        }

        if (strNames.length > idx) {
            return strNames[idx].trim();
        }
        if (!StringUtils.isBlank(valueElse)) {
            return valueElse;
        }
        return null;
    }

}
