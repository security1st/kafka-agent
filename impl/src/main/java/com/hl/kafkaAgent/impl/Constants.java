/************************************************************************************************************
* Copyright (c) 2016 Cisco and/or its affiliates.
* This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
* The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
* and/or its affiliated entities, under various laws including copyright, international treaties, patent,
* and/or contract. Any use of the material herein must be in accordance with the terms of the License.
* All rights not expressly granted by the License are reserved.
* Unless required by applicable law or agreed to separately in writing, software distributed under the
* License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
* either express or implied.
* 

* Name:       Constants.java
* Purpose:    final class maintains a list of constants
*************************************************************************************************************/

package com.hl.kafkaAgent.impl;

/**
 *
 * @author Xiaoyu Chen
 */
public final class Constants {
    
    
    
    public static final String PROP_SERIALIZATION               = "message.serialization";
    public static final String STR_SERIALIZATION_RAW            = "raw";
    public static final String STR_SERIALIZATION_AVRO           = "avro";
    public static final String PROP_KAFKA_TOPIC 		= "topic";


    // Specific section for PRODUCER
    // BEGIN Fields for reading propertie file
    //list of brokers used for bootstrapping knowledge about the rest of the cluster
    public static final String PROP_MD_BROKER_LIST              = "metadata.broker.list";   
    public static final String PROP_PARTITIONER_CLASS           = "partitioner.class";
    public static final String PROP_PRODUCER_TYPE               = "producer.type";
    public static final String PROP_PRODUCER_COMPRESSION_CODEC  = "compression.codec";
    public static final String PROP_PRODUCER_MSG_ENCODER        = "serializer.class";
    public static final String PROP_PRODUCER_COMPRESSED_TOPIC   = "compressed.topic";
    
    //kafka 0.9 properties
    public static final String PROP_BOOTSTRAP_SERVERS           = "bookstrap.servers";
    public static final String PROP_COMPRESSION_TYPE            = "compression.tpye";
    
    
    // AS: Async
    public static final String PROP_PRODUCER_AS_Q_MAX_MS        = "queue.buffering.max.ms";
    public static final String PROP_PRODUCER_AS_Q_MAX_MSG       = "queue.buffering.max.messages";
    public static final String PROP_PRODUCER_AS_Q_TIMEOUT       = "queue.enqueue.timeout.ms";
    public static final String PROP_PRODUCER_AS_Q_BATCH_MSG     = "batch.num.messages";

    public static final String STR_PRODUCER_TYPE_SYNC           = "sync";
    public static final String STR_PRODUCER_TYPE_ASYNC          = "async";
    public static final String KAFKA_DEFAULT_MSG_ENCODER        = "kafka.serializer.DefaultEncoder";
    public static final String STR_SERIALIZER_CLASS             = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String BYTEARRY_SERIALIZER_CLASS        = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String KAFKA_DATAPLATFORM_MSG_DECODER   = "com.cisco.formatter.DataplatformDecoder";

    // Specific properties for reading properties for message stucture carried in databus
    public static final String  PROP_PRODUCER_MSG_SRC           = "dataplatform.message.src";
    public static final String  PROP_PRODUCER_NIC_4_HOST_IP     = "dataplatform.message.host_ip.fromnic";
    public static final String  PROP_PRODUCER_FORMAT_IP         = "dataplatform.message.host_ip.format";
    public static final String  PROP_PRODUCER_IP_DEFAULT        = "dataplatform.message.host_ip.default";


    

    // ConfluentIO
    public static final byte TEST_MAGIC_BYTE                    = 0x0;
    public static final String  MSG_HEADER_CONFLUENT_IO         = "confluent.io";

    // Internal code config
    public static final Integer MAIN_THREAD_SLEEP_INT           = 10000;
    public static final Integer TCP_THREAD_SLEEP_INT            = 1000;
    
    
    private Constants(){}
    
}
