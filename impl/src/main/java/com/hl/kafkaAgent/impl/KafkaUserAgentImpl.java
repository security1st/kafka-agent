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

* Name:       KafkaUserAgentImpl.java
* Purpose:    Northbound kafka user agent implementation class
*************************************************************************************************************/

package com.hl.kafkaAgent.impl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opendaylight.controller.md.sal.dom.api.DOMNotification;
import org.opendaylight.controller.md.sal.dom.api.DOMNotificationListener;
import org.opendaylight.controller.md.sal.dom.api.DOMNotificationService;
import org.opendaylight.yang.gen.v1.urn.cisco.params.xml.ns.yang.messagebus.eventaggregator.rev141202.TopicId;
import org.opendaylight.yang.gen.v1.urn.cisco.params.xml.ns.yang.messagebus.eventaggregator.rev141202.TopicNotification;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.kafka.agent.rev160329.KafkaProducerConfig;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.kafka.agent.rev160329.KafkaProducerConfig.CompressionType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.params.xml.ns.yang.kafka.agent.rev160329.KafkaProducerConfig.MessageSerialization;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.common.QName;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier.NodeIdentifier;
import org.opendaylight.yangtools.yang.data.api.schema.AnyXmlNode;
import org.opendaylight.yangtools.yang.model.api.SchemaPath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 *
 * @author Xiaoyu Chen
 */
public class KafkaUserAgentImpl implements DOMNotificationListener, AutoCloseable {


    //static variables
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUserAgentImpl.class);
    private static final String AVRO_SCHEMA = "record.avsc";
    private static Schema schema;
    private static final NodeIdentifier EVENT_SOURCE_NODE = new NodeIdentifier(QName.create(TopicNotification.QNAME, "node-id"));
    private static final NodeIdentifier PAYLOAD_NODE = new NodeIdentifier(QName.create(TopicNotification.QNAME, "payload"));
    private static final NodeIdentifier TOPIC_ID_ARG = new NodeIdentifier(QName.create(TopicNotification.QNAME, "topic-id"));
    private static final SchemaPath TOPIC_NOTIFICATION_PATH = SchemaPath.create(true, TopicNotification.QNAME);


    //Private variables
    private final ListenerRegistration<KafkaUserAgentImpl> notificationReg;
    private String defaultHostIp;
    private final KafkaProducer<String, byte[]> producer;
    private final String timestampXPath;
    private final String hostIpXPath;
    private final String messageSourceXPath;
    private final String defaultMessageSource;
    private final String topic;
    private final Set<String> registeredTopics = new HashSet<>();

    /**
     * Constructor
     * @param notificationService
     * @param configuration
     */
    private KafkaUserAgentImpl(final DOMNotificationService notificationService, final KafkaProducerConfig configuration) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("in KafkaUserAgentImpl()");
        }

        LOG.info("registering to Notification Service broker");
        //register this as a listener to notification service and listens all messages published to message-bus.
        notificationReg = notificationService.registerNotificationListener(this, TOPIC_NOTIFICATION_PATH);
        LOG.info("Creating kafka producer instance using the following configuration");
        LOG.info("metadata.broker.list -> " + configuration.getKafkaBrokerList());
        LOG.info("topic -> " + configuration.getKafkaTopic());

        String topicSubscriptions = configuration.getEventSubscriptions();
        if (topicSubscriptions !=null && !topicSubscriptions.isEmpty())
        {
            LOG.info("adding topic subscriptions : " + topicSubscriptions);
            registeredTopics.addAll(Arrays.asList(topicSubscriptions.split(", ")));
        }
        topic = configuration.getKafkaTopic();
        if (topic ==null)
        {
            throw new IllegalArgumentException ("topic is a mandatory configuration. Kafka producer is not initialised.");
        }
        timestampXPath = configuration.getTimestampXpath();
        hostIpXPath = configuration.getMessageHostIpXpath();
        messageSourceXPath = configuration.getMessageSourceXpath();
        defaultMessageSource = configuration.getDefaultMessageSource();

        if (configuration.getDefaultHostIp()!=null)
        {
            setDefaultHostIP(configuration.getDefaultHostIp());
        }else{
            setDefaultHostIP("localhost");
        }

        LOG.info("default host ip is set: " + defaultHostIp);
        producer = new KafkaProducer<>(createConfig(configuration));

    }


    //Public methods --


    /**
     * Static factory method
     * @param notificationService
     * @param configuration
     * @return Singleton instance of KafkaUserAgentImpl
     */
    public static KafkaUserAgentImpl create(final DOMNotificationService notificationService, final KafkaProducerConfig configuration) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("in create()");
        }
        return new KafkaUserAgentImpl(notificationService, configuration);
    }

    /**
     * Handling notifications received via ODL message bus
     * @param notification
     */
    @Override
    public void onNotification(DOMNotification notification) {
        boolean isAcceptable = checkMsgAcceptable(notification);

        try {
            if (producer != null && isAcceptable) {
                LOG.info("onNotification: {}, {}",
                        notification.getType().toString(),
                        notification.getBody().getValue().toString());

                //processing message
                final String rawdata = this.parsePayLoad(notification);
                String messageSource = parseMessageSource(rawdata, notification);
                Long timestamp = parseMessageTimestamp(rawdata);
                String hostIp = parseHostIp(rawdata);

                LOG.debug("about to send message to Kafka ...");
                ProducerRecord<String, byte[]> message;
                if (schema == null) {
                    LOG.debug("sending data without serialization.");
                    message = new ProducerRecord<>(topic, rawdata.getBytes("UTF-8"));
                } else {
                    LOG.debug("sending data using avro serialisation.");
                    message = new ProducerRecord<>(topic, encode(timestamp, hostIp, messageSource, rawdata));
                }
                producer.send(message);
                LOG.debug("message sent.");
            }
        } catch (XPathExpressionException e) {
            LOG.error("Error while sending message to Kafka: ", e);
        } catch (ParserConfigurationException e) {
            LOG.error("Error while sending message to Kafka: ", e);
        } catch (SAXException e) {
            LOG.error("Error while sending message to Kafka: ", e);
        } catch (IOException e) {
            LOG.error("Error while sending message to Kafka: ", e);
        }
    }

    /**
     * parse host IP from rawdata
     * @param rawdata
     * @return
     * @throws XPathExpressionException
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    private String parseHostIp (String rawdata) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException
    {
        String hostIp = null;
        if (hostIpXPath != null)
        {
            LOG.debug("evaluating " + hostIpXPath + " against message payload ...");
            hostIp = this.evaluate(rawdata, hostIpXPath);
        }

        if (hostIp == null)
        {
            LOG.debug("not host ip xpath specified, use the ODL host ip by default");
            hostIp = defaultHostIp;

        }
        LOG.debug(String.format("host-ip parsed: %s" , hostIp));
        return hostIp;
    }

    /**
     * parse message source
     * @param rawdata
     * @param notification
     * @return
     * @throws XPathExpressionException
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     */
    private String parseMessageSource (String rawdata, DOMNotification notification) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException
    {
        String messageSource=null;
        if (messageSourceXPath != null)
        {
            LOG.debug(String.format("Evaluating %s against message payload...", messageSourceXPath));
            messageSource = this.evaluate(rawdata, messageSourceXPath);
        }

        if (messageSource == null)
        {
            messageSource = this.defaultMessageSource;
        }

        if (messageSource == null)
        {
            LOG.debug("no message source xpath specified or invalid xpath statement. Use the node-id by default.");
            final String nodeId = notification.getBody().getChild(EVENT_SOURCE_NODE).get().getValue().toString();
            messageSource = nodeId;
        }
        LOG.debug(String.format("message source parsed: %s", messageSource));
        return messageSource;
    }

    private Long parseMessageTimestamp (String rawdata) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException
    {
        Long timestamp = null;
        if (timestampXPath != null)
        {
            LOG.debug("evaluating " + timestampXPath + " against message payload ...");
            timestamp = Long.valueOf(this.evaluate(rawdata, timestampXPath));

        }

        if (timestamp == null)
        {
            LOG.debug("no timestampe xpath specified or invalid xpath statement. Use the system time by default");
            timestamp = System.currentTimeMillis();
        }

        LOG.debug("timestamp parsed: " + timestamp);

        return timestamp;

    }

    private boolean checkMsgAcceptable (DOMNotification notification)
    {
        if (registeredTopics.isEmpty())
        {
            return true;
        }

        LOG.debug("topic filters are not empty; applying them now...");
        //check topic against filter list
        if(notification.getBody().getChild(TOPIC_ID_ARG).isPresent()){
            TopicId topicId = (TopicId) notification.getBody().getChild(TOPIC_ID_ARG).get().getValue();
            if (registeredTopics.contains(topicId.getValue()))
            {
                LOG.debug("topic is parsed: " + topicId.getValue());
                LOG.debug("Topic accepted.");
                return true;
            }
        }
        return false;
    }

    /**
     * Stop kafka agent
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("in close()");
        }
        //unregister notification handler
        notificationReg.close();

        //stop kafka producer
        producer.close();
    }

    //Private methods --

    /**
     * set default host ip
     * @param defaultHostIp
     */
    private void setDefaultHostIP (String defaultHostIp)
    {
        this.defaultHostIp = defaultHostIp;
    }

    /**
     * encode raw message to avro format
     * @param timestamp
     * @param hostIp
     * @param src
     * @param payload
     * @return
     * @throws IOException
     */
    private byte[] encode(Long timestamp, String hostIp, String src, String payload) throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        GenericRecord datum = new GenericData.Record(schema);
        datum.put("src", src);
        datum.put("timestamp", timestamp);
        datum.put("host_ip", hostIp);
        datum.put("rawdata", ByteBuffer.wrap(payload.getBytes("UTF-8")));
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();
        return out.toByteArray();
    }

    /**
     * Create kafka producer configuration
     * @param configuration
     * @return
     */
    private static Properties createConfig(KafkaProducerConfig configuration) {
        Properties props = new Properties();
        //check existence of mandatory configurations
        checkConfigValidity(configuration);
        //set producer properties for kafka version 0.8 or lower
        props.put(""+ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  configuration.getKafkaBrokerList());
        setCompressionTypeProp(configuration.getCompressionType(), props);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Constants.STR_SERIALIZER_CLASS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Constants.BYTEARRY_SERIALIZER_CLASS);
        LOG.info("setting serialization property ...");
        initSchema(configuration.getMessageSerialization(), configuration.getAvroSchemaNamespace());
        LOG.info("creating ProducerConfig instance...");
        return props;
    }

    /**
     * initialize Avro schema
     * @param ser
     * @param ns
     */
    private static void initSchema (MessageSerialization ser, String ns)
    {
        switch(ser.getIntValue())
        {
            case 0:
                    LOG.info("No serialization set.");
                    schema = null;
                    break;
            case 1:
                    LOG.info("load schema from classpath  ...");

                    schema = new Schema.Parser().parse(loadAvroSchemaAsString(ns));
                    LOG.info("schema loaded.");
                    break;
            default:
                    throw new IllegalArgumentException("Unrecognised message serialization type " + ser.getIntValue()+". Kafka producer is not intialised.");
        }
    }

    /**
     * set compression type property
     * @param type
     * @param props
     */
    private static void setCompressionTypeProp(CompressionType type, Properties props)
    {
        LOG.info("setting compression codec property ...");
        switch(type.getIntValue())
        {
            case 0: props.put(""+ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
                    break;
            case 1: props.put(""+ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
                    break;
            case 2: props.put(""+ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
                    break;
            default: throw new IllegalArgumentException("Unrecognised/unsupported compression encoding " + type.getIntValue()+". Kafka producer is not intialised.");
        }
    }

    /**
     * check validity of kafka producer configurations
     * @param configuration
     */
    private static void checkConfigValidity(KafkaProducerConfig configuration)
    {
        boolean avroNsDefined = configuration.getAvroSchemaNamespace() != null;
        if (!avroNsDefined) {
            throw new IllegalArgumentException("Kafka producer is not initialised due to undefined 'avro-schema-namespace'.");
        }

        boolean brokerListDefined = configuration.getKafkaBrokerList()!=null;
        if (!brokerListDefined) {
            throw new IllegalArgumentException("Kafka producer is not initialised due to undefined 'kafka-broker-list'.");
        }
        boolean compressionTypeDefined = configuration.getCompressionType()!=null;
        if (!compressionTypeDefined) {
            throw new IllegalArgumentException("Kafka producer is not initialised due to undefined 'compression-type'.");
        }
        boolean msgSerializationDefined = configuration.getMessageSerialization()!=null;
        if (!msgSerializationDefined) {
            throw new IllegalArgumentException("Kafka producer is not initialised due to undefined 'message-serialization'.");
        }
    }

    /**
     * Extract raw event payload
     * @param notification
     * @return
     */
    private String parsePayLoad(DOMNotification notification){

        if (LOG.isDebugEnabled())
        {
            LOG.debug("in parsePayLoad");
        }
        final AnyXmlNode encapData = (AnyXmlNode) notification.getBody().getChild(PAYLOAD_NODE).get();

        final StringWriter writer = new StringWriter();
        final StreamResult result = new StreamResult(writer);
        final TransformerFactory tf = TransformerFactory.newInstance();
        try {
        final Transformer transformer = tf.newTransformer();
            transformer.transform(encapData.getValue(), result);
        } catch (TransformerException e) {
            LOG.error("Can not parse PayLoad data", e);
            return null;
        }
        writer.flush();
        return writer.toString();
    }

    /**
     * Utility function that loads XML document from string
     * @param xml
     * @return XML Document
     * @throws Exception
     */
    private static Document loadXmlFromString (String xml) throws ParserConfigurationException, SAXException, IOException
    {
        DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
        fac.setNamespaceAware(false);
        DocumentBuilder builder;
        builder = fac.newDocumentBuilder();
        return builder.parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
    }

    /**
     * Evaluate XPath
     * @param payload
     * @param xpathStmt
     * @return
     * @throws Exception
     */
    private String evaluate (String payload, String xpathStmt) throws XPathExpressionException, ParserConfigurationException, SAXException, IOException
    {
        String result = null;

        Document doc = loadXmlFromString(payload);

        XPath xpath = XPathFactory.newInstance().newXPath();
        NodeList nodeList = (NodeList) xpath.evaluate(xpathStmt, doc, XPathConstants.NODESET);

        if (nodeList.getLength()>0)
        {
            Node node = nodeList.item(0);
            result = node.getTextContent();
        }

        return result;
    }

    /**
     * Load avro schema and replace namespace with user-defined value
     * @param namespace
     * @return avro schema
     * @throws Exception
     */
    private static String loadAvroSchemaAsString (String namespace)
    {
        String avroSchemaStr;
        StringBuilder sb = new StringBuilder("");
        InputStream is = KafkaUserAgentImpl.class.getClassLoader().getResourceAsStream(AVRO_SCHEMA);
        try (Scanner scanner = new Scanner(is, "UTF-8")) {
            while (scanner.hasNextLine())
            {
                String line = scanner.nextLine();
                sb.append(line);
            }
        }
        avroSchemaStr = String.format(sb.toString(), namespace);
        return avroSchemaStr;
    }


}
