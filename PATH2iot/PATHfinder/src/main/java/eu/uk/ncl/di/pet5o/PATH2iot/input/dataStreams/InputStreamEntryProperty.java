package eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams;

/**
 * Created by peto on 20/02/2017.
 *
 * Most inner class of input_streams json.
 */
public class InputStreamEntryProperty {
    private String name;
    private String asName;
    private String type;

    public InputStreamEntryProperty() {}

    public InputStreamEntryProperty(String name, String type) {
        this.name = name;
        this.asName = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        // attempt to follow same types as DotExpression is
//       if (type.equals("String")) {
//           return "java.lang.String";
//       } else if (type.equals("Integer")) {
//           return "java.lang.Integer";
//       } else if (type.equals("Double")) {
//           return "java.lang.Double";
//       } else if (type.equals("Long")) {
//           return "java.lang.Long";
//       } else {
//           return "unknown";
//       }
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }
}
