/**
 * JSON SerDe for Hive
 */
package org.apache.hadoop.hive.contrib.serde2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * JSON SerDe for Hive
 * <p>
 * This SerDe can be used to read data in JSON format from HDFS or S3 (if you
 * are using AWS). For example, if your JSON files had the following contents:
 * 
 * <pre>
 * {"field1":"data1","field2":100,"field3":"more data1"}
 * {"field1":"data2","field2":200,"field3":"more data2"}
 * {"field1":"data3","field2":300,"field3":"more data3"}
 * {"field1":"data4","field2":400,"field3":"more data4"}
 * </pre>
 * 
 * The following steps can be used to read this data:
 * <ol>
 * <li>Build this project using <code>ant build</code></li>
 * <li>Copy <code>hive-json-serde.jar</code> to the Hive server</li>
 * <li>Inside the Hive client, run
 * 
 * <pre>
 * ADD JAR /home/hadoop/hive-json-serde.jar;
 * </pre>
 * 
 * </li>
 * <li>Create a table that uses files where each line is JSON object
 * 
 * <pre>
 * CREATE EXTERNAL TABLE IF NOT EXISTS my_table (
 *    field1 string, field2 int, field3 string
 * )
 * ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.JsonSerde'
 * LOCATION 's3://my_data/my_table/';
 * </pre>
 * 
 * </li>
 * <li>Copy your JSON files to <code>s3://my_data/my_table/</code>. You can now
 * select data using normal SELECT statements
 * 
 * <pre>
 * SELECT * FROM my_table LIMIT 10;
 * </pre>
 * 
 * 
 * </li>
 * </ol>
 * <p>
 * The table does not have to have the same columns as the JSON files, and
 * vice-versa. If the table has a column that does not exist in the JSON object,
 * it will have a NULL value. If the JSON file contains fields that are not
 * columns in the table, they will be ignored and not visible to the table.
 * 
 * 
 * @see <a href="http://code.google.com/p/hive-json-serde/">hive-json-serde on
 *      Google Code</a>
 * @author Peter Sankauskas
 */
public class JsonSerde implements SerDe {
    /**
     * Apache commons logger
     */
    private static final Log LOG = LogFactory.getLog(JsonSerde.class.getName());

    /**
     * The number of columns in the table this SerDe is being used with
     */
    private int numColumns;

    /**
     * List of column names in the table
     */
    private List<String> columnNames;

    /**
     * An ObjectInspector to be used as meta-data about a deserialized row
     */
    private StructObjectInspector rowOI;

    /**
     * A row object
     */
    private ArrayList<Object> row;

    /**
     * Initialize this SerDe with the system properties and table properties
     * 
     */
    @Override
    public void initialize(Configuration sysProps, Properties tblProps)
	    throws SerDeException {
	LOG.debug("Initializing JsonSerde");

	// Get the names of the columns for the table this SerDe is being used
	// with
	String columnNameProperty = tblProps
		.getProperty(Constants.LIST_COLUMNS);
	columnNames = Arrays.asList(columnNameProperty.split(","));

	// Convert column types from text to TypeInfo objects
	String columnTypeProperty = tblProps
		.getProperty(Constants.LIST_COLUMN_TYPES);
	List<TypeInfo> columnTypes = TypeInfoUtils
		.getTypeInfosFromTypeString(columnTypeProperty);
	assert columnNames.size() == columnTypes.size();
	numColumns = columnNames.size();

	// Create ObjectInspectors from the type information for each column
	List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(
		columnNames.size());
	ObjectInspector oi;
	for (int c = 0; c < numColumns; c++) {
	    oi = TypeInfoUtils
		    .getStandardJavaObjectInspectorFromTypeInfo(columnTypes
			    .get(c));
	    columnOIs.add(oi);
	}
	rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(
		columnNames, columnOIs);

	// Create an empty row object to be reused during deserialization
	row = new ArrayList<Object>(numColumns);
	for (int c = 0; c < numColumns; c++) {
	    row.add(null);
	}

	LOG.debug("JsonSerde initialization complete");
    }

    /**
     * Gets the ObjectInspector for a row deserialized by this SerDe
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
	return rowOI;
    }

    /**
     * Deserialize a JSON Object into a row for the table
     */
    @Override
    public Object deserialize(Writable blob) throws SerDeException {
	Text rowText = (Text) blob;
	LOG.debug("Deserialize row: " + rowText.toString());

	// Try parsing row into JSON object
	JSONObject jObj;
	try {
	    jObj = new JSONObject(rowText.toString());
	} catch (JSONException e) {
	    // If row is not a JSON object, make the whole row NULL
	    LOG.error("Row is not a valid JSON Object - JSONException: "
		    + e.getMessage());
	    return null;
	}

	// Loop over columns in table and set values
	String colName;
	Object value;
	for (int c = 0; c < numColumns; c++) {
	    colName = columnNames.get(c);

	    try {
		value = jObj.get(colName);
	    } catch (JSONException e) {
		// If the column cannot be found, just make it a NULL value and
		// skip over it
		LOG.warn("Column '" + colName + "' not found in row: "
			+ rowText.toString() + " - JSONException: "
			+ e.getMessage());
		value = null;
	    }
	    row.set(c, value);
	}

	return row;
    }

    /**
     * Not sure - something to do with serialization of data
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
	return Text.class;
    }

    /**
     * Serializes a row of data into a JSON object
     * 
     * @todo Implement this - sorry!
     */
    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector)
	    throws SerDeException {
	LOG.info("-----------------------------");
	LOG.info("--------- serialize ---------");
	LOG.info("-----------------------------");
	LOG.info(obj.toString());
	LOG.info(objInspector.toString());

	return null;
    }
}