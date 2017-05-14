package org.apache.flink.quickstart;

import es.accenture.flink.Sink.KuduSink;
import es.accenture.flink.Utils.RowSerializable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Class used for tests which contains usefull function to read, write and insert data into a kudu database
 */

class TestUtils {

    public void doJob(
            DataStream<String> stream,
            String kuduMaster,
            String table,
            String [] columnNames) throws Exception  {

        DataStream<RowSerializable> stream2 = stream.map(new TestUtils.MyMapFunction2());
        stream2.print();
        stream2.addSink(new KuduSink(kuduMaster, table, columnNames));

    }

    /**
     * Map function which receives a row and makes some changes. For example, multiplies the key field by 2
     * and changes value field to upper case
     */
    public static class MyMapFunction implements MapFunction<RowSerializable, RowSerializable> {

        @Override
        public RowSerializable map(RowSerializable row) throws Exception {

            for (int i = 0; i < row.productArity(); i++) {
                if (row.productElement(i).getClass().equals(String.class))
                    row.setField(1, row.productElement(1).toString().toUpperCase());
                else if (row.productElement(i).getClass().equals(Integer.class))
                    row.setField(0, (Integer)row.productElement(0)*2);
            }
            return row;
        }
    }


    /**
     * Map function which receives a String, splits it, and creates as many row as word has the string
     * This row contains two fields, first field is a serial generated automatically starting in 0,
     * second field is the substring generated by the split function
     *
     */
    public static class MyMapFunction2 implements MapFunction<String, RowSerializable>{

        @Override
        public RowSerializable map(String input) throws Exception {

            RowSerializable res = new RowSerializable(2);
            Integer i = 0;
            for (String s : input.split(" ")) {
                /*Needed to prevent exception on map function if phrase has more than 4 words*/
                if(i<3)
                    res.setField(i, s.toUpperCase());
                i++;
            }
            return res;
        }
    }

    /**
     * Creates a Kudu table for tests
     *
     * @param client KuduClient
     * @param tableName Name of the table
     * @param mode "INT" if the key field is an Integer, "STRING" if it is a String
     * @return True if the table has been created successfully, False if not
     */

    static boolean createTable(KuduClient client, String tableName, String mode) {

        try{
            ArrayList columns = new ArrayList(2);
            if(mode.equals("INT")){
                columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32)
                        .key(true)
                        .build());
            }else if (mode.equals("STRING")){
                columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.STRING)
                        .key(true)
                        .build());
            }else{
                return false;
            }

            columns.add(new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING)
                    .build());
            List<String> rangeKeys = new ArrayList<>();
            rangeKeys.add("key");
            Schema schema = new Schema(columns);
            client.createTable(tableName, schema,
                    new CreateTableOptions().setRangePartitionColumns(rangeKeys).addHashPartitions(rangeKeys, 4));
            return true;
        } catch(Exception e){
            /*Print stack disable for JUnit*/
            return false;
        }
    }

    /**
     * Inserts as many rows as numRows into the table given
     *
     * @param client KuduClient
     * @param tableName Name of the table
     * @param numRows Number of rows will be created
     * @return True if the data has been inserted successfully, False if not
     */

    static boolean insertData(KuduClient client, String tableName, int numRows){

        try {

            KuduTable table = client.openTable(tableName);
            KuduSession session = client.newSession();
            for (int i = 0; i < numRows; i++) {
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addInt(0, i);
                row.addString(1, "This is the row number: "+ i);
                session.apply(insert);
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Return a list of scanned rows
     *
     * @param client KuduClient
     * @param tableName Name of the table
     * @return List of scanned rows
     */
    static ArrayList<String> scanRows(KuduClient client,String tableName){

        ArrayList<String> array = new ArrayList<>();
        try {
            KuduTable table = client.openTable(tableName);
            table.getSchema();
            List<String> projectColumns = new ArrayList<>(2);
            projectColumns.add("key");
            projectColumns.add("value");

            KuduScanner scanner = client.newScannerBuilder(table)
                    .setProjectedColumnNames(projectColumns)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    array.add(result.rowToString());
                }
            }
            return array;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Returns how many rows are in the table
     *
     * @param client KuduClient
     * @param tableName Name of the table
     * @return Number of rows
     */

    static int numRows(KuduClient client,String tableName){

        Integer cont = 0;
        try {
            KuduTable table = client.openTable(tableName);
            table.getSchema();
            List<String> projectColumns = new ArrayList<>(2);
            projectColumns.add("key");
            projectColumns.add("value");

            KuduScanner scanner = client.newScannerBuilder(table)
                    .setProjectedColumnNames(projectColumns)
                    .build();
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    cont++;
                }
            }
            return cont;
        } catch (Exception e) {
            return -1;
        }
    }

}
