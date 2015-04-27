package interretis.sparktraining.counties;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CountyRecord {

    public static CountyRecord fromLine(final String line) {

        final String[] tokens = line.split(",");

        final String county = tokens[0];
        final String state = tokens[1];

        final long population = (long) Double.parseDouble(tokens[2]);
        final long housingUnits = (long) Double.parseDouble(tokens[3]);

        final double totalArea = Double.parseDouble(tokens[4]);
        final double waterArea = Double.parseDouble(tokens[5]);
        final double landArea = Double.parseDouble(tokens[6]);

        final double densityPop = Double.parseDouble(tokens[7]);
        final double densityHousing = Double.parseDouble(tokens[8]);

        return new CountyRecord(county, state, population, housingUnits, totalArea, waterArea, landArea, densityPop, densityHousing);
    }

    private String county;
    private String state;

    private long population;
    private long housingUnits;

    private double totalArea;
    private double waterArea;
    private double landArea;

    private double densityPop;
    private double densityHousing;
}
