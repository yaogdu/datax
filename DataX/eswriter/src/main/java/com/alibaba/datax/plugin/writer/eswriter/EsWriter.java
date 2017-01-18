package com.alibaba.datax.plugin.writer.eswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dear on 2017/1/17.
 */
public class EsWriter extends Writer {

    public static class Job extends Writer.Job {

        private static final Logger logger = LoggerFactory.getLogger(Job.class);
        private Configuration originalConfig = null;

        @Override
        public void preCheck() {
            super.preCheck();
        }

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {

            List<Configuration> configList = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configList.add(this.originalConfig.clone());
            }
            return configList;
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger logger = LoggerFactory.getLogger(Task.class);

        SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

        String esIndex = "";

        String esType = "";

        static int BATCH_SIZE = 1000;

        Configuration writerSliceConfig;

        TransportClient client = null;

        String clusterName = "";


        private JSONArray columnMeta = null;

        @Override
        public void prepare() {
            super.prepare();
        }

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.esIndex = writerSliceConfig.getString(Key.INDEX);
            this.esType = writerSliceConfig.getString(Key.TYPE);
            int batchSize = writerSliceConfig.getInt(Key.BATCH_SIZE);
            if (batchSize > 0) {
                BATCH_SIZE = batchSize;
            }

            clusterName = writerSliceConfig.getString(Key.CLUSTER_NAME);
            String hostArray = writerSliceConfig.getString(Key.HOST);

            this.columnMeta = JSON.parseArray(writerSliceConfig.getString(Key.COLUMN));

            int port = writerSliceConfig.getInt(Key.PORT);
            try {

                String[] hosts = hostArray.split(",");

                if (null == hosts || hosts.length <= 0) {
                    throw new UnknownHostException("host config error");
                }

                Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();

                TransportClient client = TransportClient.builder().settings(settings).build();

                for (int i = 0; i < hosts.length; i++) {
                    String host = hosts[i];
                    client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                }

                this.client = client;


            } catch (UnknownHostException e) {
                logger.error("es host error", e);
                System.exit(1);
            }

        }

        @Override
        public void destroy() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {

            try {
                if (("").equals(esIndex) || ("").equals(esType)) {
                    throw new Exception("es index and es type can not be empty");
                }


                List<Record> writerBuffer = new ArrayList<Record>(BATCH_SIZE);
                Record record = null;
                while ((record = lineReceiver.getFromReader()) != null) {
                    writerBuffer.add(record);
                    if (writerBuffer.size() >= BATCH_SIZE) {
                        batchInsert(writerBuffer, columnMeta);
                        writerBuffer.clear();
                    }
                }
                if (!writerBuffer.isEmpty()) {
                    batchInsert(writerBuffer, columnMeta);
                    writerBuffer.clear();
                }

            } catch (Exception e) {
                logger.error("writer error", e);
            }
        }

        private void batchInsert(List<Record> writerBuffer, JSONArray columnMeta) {

            try {
                for (Record record : writerBuffer) {

                    Map<String, Object> document = new java.util.HashMap<String, Object>();

                    for (int i = 0; i < record.getColumnNumber(); i++) {
                        String type = columnMeta.getJSONObject(i).getString(Key.COLUMN_TYPE);
                        String columnName = columnMeta.getJSONObject(i).getString(Key.COLUMN_NAME);

//                        if ("complete_time".equalsIgnoreCase(columnName)) {
//                            logger.info("type is {} and columnName is {} and value is {}", type, columnName, record.getColumn(i).getRawData());
//                        }
                        if (("decimal").equalsIgnoreCase(type)) {

                            Column col = record.getColumn(i);

                            if (col == null || col.asBigDecimal() == null) {
                                document.put(convert(columnName), null);
                            } else {
                                document.put(convert(columnName), record.getColumn(i).asBigDecimal().multiply(new BigDecimal(100)).longValue());
                            }

                        } else if (type.equalsIgnoreCase(Column.Type.DATE.name())) {
                            Column col = record.getColumn(i);

                            //logger.info("column name is {} and column value is {} and num is {}", columnName, col.getRawData(), document.get("num"));

                            if (null == col.asDate()) {
                                document.put(convert(columnName), null);
                            } else {
                                document.put(convert(columnName), dateFormat.format(record.getColumn(i).asDate()));
                            }
                        } else {
                            document.put(convert(columnName), record.getColumn(i).getRawData());
                        }

                    }

                    IndexResponse response = client.prepareIndex(esIndex, esType, document.get("num").toString()).setSource(document).get();

                }

//                logger.info("bulkRequest.numofactions {}", bulkRequest.numberOfActions());
//                BulkResponse bulkResponse = bulkRequest.get();
//
//                logger.info("bulkresponse.length is {}", bulkResponse.getItems().length);
//
//                if (bulkResponse.hasFailures()) {
//                    logger.info(bulkResponse.buildFailureMessage());
//                    throw new Exception("batchInsert error");
//                }
            } catch (Exception e) {
                logger.error("batch insert error", e);
            }

        }


        private String convert(String oldFieldName) {
            if (oldFieldName == null || "".equals(oldFieldName.trim())) {
                return "";
            }
            StringBuilder sb = new StringBuilder(oldFieldName);
            Matcher mc = Pattern.compile("_").matcher(oldFieldName);
            int i = 0;
            while (mc.find()) {
                int position = mc.end() - (i++);
                sb.replace(position - 1, position + 1, sb.substring(position, position + 1).toUpperCase());
            }
            return sb.toString();
        }

    }
}
