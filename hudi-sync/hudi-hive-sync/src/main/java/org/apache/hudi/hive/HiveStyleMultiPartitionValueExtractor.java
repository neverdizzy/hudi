package org.apache.hudi.hive;

import org.apache.hudi.common.util.ValidationUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author bo.chen
 * @date 2022/6/10
 **/
public class HiveStyleMultiPartitionValueExtractor implements PartitionValueExtractor {
    private static final long serialVersionUID = 1L;

    @Override
    public List<String> extractPartitionValuesInPath(String partitionPath) {
        // If the partitionPath is empty string( which means none-partition table), the partition values
        // should be empty list.
        if (partitionPath.isEmpty()) {
            return Collections.emptyList();
        }

        String[] splits = partitionPath.split("/");
        return Arrays.stream(splits).map(s -> {
            if (s.contains("=")) {
                String[] moreSplit = s.split("=");
                ValidationUtils.checkArgument(moreSplit.length == 2, "Partition Field (" + s + ") not in expected format");
                return moreSplit[1];
            }
            return s;
        }).collect(Collectors.toList());

        // partition path is expected to be in this format partition_key=partition_value.
        // String[] splits = partitionPath.split("=");
        // if (splits.length != 2) {
        //     throw new IllegalArgumentException(
        //             "Partition path " + partitionPath + " is not in the form partition_key=partition_value.");
        // }
        // return Collections.singletonList(splits[1]);
    }
}
