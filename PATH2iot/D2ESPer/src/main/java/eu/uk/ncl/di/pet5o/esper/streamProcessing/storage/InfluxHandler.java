package eu.uk.ncl.di.pet5o.esper.streamProcessing.storage;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

/**
 * Influxdb handler to store the data in database.
 */
public class InfluxHandler {

    private static Logger logger = LogManager.getLogger(InfluxHandler.class);

    public static void persistResult(double count, String measurement, String field) {
        logger.debug(String.format("Received a message: %f, %s, %s.", count, measurement, field));

        // todo load influxdb credentials from config
        InfluxDB influxDB = InfluxDBFactory.connect("http://localhost:8086", "root", "root");
        influxDB.setDatabase(measurement);

        Point point = Point.measurement(measurement)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField(field, count)
                .build();

        influxDB.write(point);

        influxDB.close();
    }
}
