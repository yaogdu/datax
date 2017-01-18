package com.alibaba.datax.plugin.writer.eswriter;

/**
 * Created by dear on 2017/1/17.
 */
public interface Key {

    /**
     * es cluster name
     */
    String CLUSTER_NAME = "clusterName";


    /**
     * es index
     */
    String INDEX = "esIndex";

    /**
     * es type
     */
    String TYPE = "esType";

    /**
     * batch size
     */
    String BATCH_SIZE = "batchSize";

    /**
     * es host
     */
    String HOST = "host";

    String PORT = "port";

    String COLUMN = "column";

    String COLUMN_TYPE = "type";
    String COLUMN_NAME = "name";
}
