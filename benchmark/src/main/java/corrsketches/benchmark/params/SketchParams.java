package corrsketches.benchmark.params;

import corrsketches.SketchType;

import java.util.ArrayList;
import java.util.List;

public class SketchParams {

    public final SketchType type;
    public final double budget;

    public SketchParams(SketchType type, double budget) {
        this.type = type;
        this.budget = budget;
    }

    public static List<SketchParams> parse(String params) {
        String[] values = params.split(",");
        List<SketchParams> result = new ArrayList<>();
        for (String value : values) {
            result.add(parseValue(value.trim()));
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException(
                    String.format("[%s] does not have any valid sketch parameters", params));
        }
        return result;
    }

    public static SketchParams parseValue(String params) {
        String[] values = params.split(":");
        if (values.length == 2) {
            return new SketchParams(
                    SketchType.valueOf(values[0].trim()), Double.parseDouble(values[1].trim()));
        } else {
            throw new IllegalArgumentException(String.format("[%s] is not a valid parameter", params));
        }
    }

    @Override
    public String toString() {
        return type.toString() + ":" + budget;
    }
}
