package com.zendesk.maxwell.row;

import com.fasterxml.jackson.core.*;
import com.zendesk.maxwell.errors.ProtectedAttributeNameException;
import com.zendesk.maxwell.producer.EncryptionMode;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.producer.MaxwellOutputConfig;
import com.zendesk.maxwell.replication.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;


public class RowMap implements Serializable {

	public enum KeyFormat { HASH, ARRAY }

	static final Logger LOGGER = LoggerFactory.getLogger(RowMap.class);

	private final String rowQuery;
	private final String rowType;
	private final String database;
	private final String table;
	private final Long timestampMillis;
	private final Long timestampSeconds;
	private Position nextPosition;

	private Long xid;
	private boolean txCommit;
	private Long serverId;
	private Long threadId;

	private final LinkedHashMap<String, Object> data;
	private final LinkedHashMap<String, Object> oldData;

	private final LinkedHashMap<String, Object> extraAttributes;

	private final List<String> pkColumns;

	private static final JsonFactory jsonFactory = new JsonFactory();

	private long approximateSize;

	private static final ThreadLocal<ByteArrayOutputStream> byteArrayThreadLocal =
			new ThreadLocal<ByteArrayOutputStream>(){
				@Override
				protected ByteArrayOutputStream initialValue() {
					return new ByteArrayOutputStream();
				}
			};

	private static JsonGenerator resetJsonGenerator() {
		byteArrayThreadLocal.get().reset();
		return jsonGeneratorThreadLocal.get();
	}

	private static final ThreadLocal<JsonGenerator> jsonGeneratorThreadLocal =
			new ThreadLocal<JsonGenerator>() {
				@Override
				protected JsonGenerator initialValue() {
					JsonGenerator g = null;
					try {
						g = jsonFactory.createGenerator(byteArrayThreadLocal.get());
					} catch (IOException e) {
						LOGGER.error("error initializing jsonGenerator", e);
						return null;
					}
					g.setRootValueSeparator(null);
					return g;
				}
			};

	private static final ThreadLocal<DataJsonGenerator> plaintextDataGeneratorThreadLocal =
			new ThreadLocal<DataJsonGenerator>() {
				@Override
				protected DataJsonGenerator initialValue() {
					return new PlaintextJsonGenerator(jsonGeneratorThreadLocal.get());
				}
			};

	private static final ThreadLocal<EncryptingJsonGenerator> encryptingJsonGeneratorThreadLocal =
			new ThreadLocal<EncryptingJsonGenerator>() {
				@Override
				protected EncryptingJsonGenerator initialValue(){
					try {
						return new EncryptingJsonGenerator(jsonGeneratorThreadLocal.get(),jsonFactory);
					} catch (IOException e) {
						LOGGER.error("error initializing EncryptingJsonGenerator", e);
						return null;
					}
				}
			};

	public RowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
			Position nextPosition, String rowQuery) {
		this.rowQuery = rowQuery;
		this.rowType = type;
		this.database = database;
		this.table = table;
		this.timestampMillis = timestampMillis;
		this.timestampSeconds = timestampMillis / 1000;
		this.data = new LinkedHashMap<>();
		this.oldData = new LinkedHashMap<>();
		this.extraAttributes = new LinkedHashMap<>();
		this.nextPosition = nextPosition;
		this.pkColumns = pkColumns;
		this.approximateSize = 100L; // more or less 100 bytes of overhead
	}

	public RowMap(String type, String database, String table, Long timestampMillis, List<String> pkColumns,
				  Position nextPosition) {
		this(type, database, table, timestampMillis, pkColumns, nextPosition, null);
	}

	//Do we want to encrypt this part?
	public String pkToJson(KeyFormat keyFormat) throws IOException {
		if ( keyFormat == KeyFormat.HASH )
			return pkToJsonHash();
		else
			return pkToJsonArray();
	}

	private String pkToJsonHash() throws IOException {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, database);
		g.writeStringField(FieldNames.TABLE, table);

		if (pkColumns.isEmpty()) {
			g.writeStringField(FieldNames.UUID, UUID.randomUUID().toString());
		} else {
			for (String pk : pkColumns) {
				Object pkValue = null;
				if ( data.containsKey(pk) )
					pkValue = data.get(pk);

				g.writeObjectField("pk." + pk.toLowerCase(), pkValue);
			}
		}

		g.writeEndObject(); // end of 'data: { }'
		g.flush();
		return jsonFromStream();
	}

	private String pkToJsonArray() throws IOException {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartArray();
		g.writeString(database);
		g.writeString(table);

		g.writeStartArray();
		for (String pk : pkColumns) {
			Object pkValue = null;
			if ( data.containsKey(pk) )
				pkValue = data.get(pk);

			g.writeStartObject();
			g.writeObjectField(pk.toLowerCase(), pkValue);
			g.writeEndObject();
		}
		g.writeEndArray();
		g.writeEndArray();
		g.flush();
		return jsonFromStream();
	}

	public String pkAsConcatString() {
		if (pkColumns.isEmpty()) {
			return database + table;
		}
		StringBuilder keys = new StringBuilder();
		for (String pk : pkColumns) {
			Object pkValue = null;
			if (data.containsKey(pk))
				pkValue = data.get(pk);
			if (pkValue != null)
				keys.append(pkValue.toString());
		}
		if (keys.length() == 0)
			return "None";
		return keys.toString();
	}

	public String buildPartitionKey(List<String> partitionColumns) {
		StringBuilder partitionKey= new StringBuilder();
		for (String pc : partitionColumns) {
			Object pcValue = null;
			if (data.containsKey(pc))
				pcValue = data.get(pc);
			if (pcValue != null)
				partitionKey.append(pcValue.toString());
		}

		return partitionKey.toString();
	}

	private void writeMapToJSON(
			String jsonMapName,
			LinkedHashMap<String, Object> data,
			JsonGenerator g,
			boolean includeNullField,
			String keyCase
	) throws IOException, NoSuchAlgorithmException {
		g.writeObjectFieldStart(jsonMapName);

		for (String key : data.keySet()) {
			Object value = data.get(key);

			if (value == null && !includeNullField)
				continue;

			if (value instanceof List) { // sets come back from .asJSON as lists, and jackson can't deal with lists natively.
				List stringList = (List) value;

				if ("lower".equalsIgnoreCase(keyCase)) {
					g.writeArrayFieldStart(key.toLowerCase());
				} else if ("upper".equalsIgnoreCase(keyCase)) {
					g.writeArrayFieldStart(key.toUpperCase());
				} else {
					g.writeArrayFieldStart(key);
				}
				for (Object s : stringList) {
					g.writeObject(s);
				}
				g.writeEndArray();
			} else if (value instanceof RawJSONString) {
				// JSON column type, using binlog-connector's serializers.
				if ("lower".equalsIgnoreCase(keyCase)) {
					g.writeFieldName(key.toLowerCase());
				} else if ("upper".equalsIgnoreCase(keyCase)) {
					g.writeFieldName(key.toUpperCase());
				} else {
					g.writeFieldName(key);
				}
				g.writeRawValue(((RawJSONString) value).json);
			} else {
				if ("lower".equalsIgnoreCase(keyCase)) {
					g.writeObjectField(key.toLowerCase(), value);
				} if ("upper".equalsIgnoreCase(keyCase)) {
					g.writeObjectField(key.toUpperCase(), value);
				} else {
					g.writeObjectField(key, value);
				}
			}
		}

		g.writeEndObject(); // end of 'jsonMapName: { }'
	}

	public String toJSON() throws Exception {
		return toJSON(new MaxwellOutputConfig());
	}

	public String toJSON(MaxwellOutputConfig outputConfig) throws Exception {
		JsonGenerator g = resetJsonGenerator();

		g.writeStartObject(); // start of row {

		g.writeStringField(FieldNames.DATABASE, this.database);
		g.writeStringField(FieldNames.TABLE, this.table);

		if ( outputConfig.includesRowQuery && this.rowQuery != null) {
			g.writeStringField(FieldNames.QUERY, this.rowQuery);
		}

		g.writeStringField(FieldNames.TYPE, this.rowType);
		g.writeNumberField(FieldNames.TIMESTAMP, this.timestampSeconds);

		if ( outputConfig.includesCommitInfo ) {
			if ( this.xid != null )
				g.writeNumberField(FieldNames.TRANSACTION_ID, this.xid);

			if ( this.txCommit )
				g.writeBooleanField(FieldNames.COMMIT, true);
		}

		BinlogPosition binlogPosition = this.nextPosition.getBinlogPosition();
		if ( outputConfig.includesBinlogPosition )
			g.writeStringField(FieldNames.POSITION, binlogPosition.getFile() + ":" + binlogPosition.getOffset());


		if ( outputConfig.includesGtidPosition)
			g.writeStringField(FieldNames.GTID, binlogPosition.getGtid());

		if ( outputConfig.includesServerId && this.serverId != null ) {
			g.writeNumberField(FieldNames.SERVER_ID, this.serverId);
		}

		if ( outputConfig.includesThreadId && this.threadId != null ) {
			g.writeNumberField(FieldNames.THREAD_ID, this.threadId);
		}

		for ( Map.Entry<String, Object> entry : this.extraAttributes.entrySet() ) {
			g.writeObjectField(entry.getKey(), entry.getValue());
		}

		if ( outputConfig.excludeColumns.size() > 0 ) {
			// NOTE: to avoid concurrent modification.
			Set<String> keys = new HashSet<>();
			keys.addAll(this.data.keySet());
			keys.addAll(this.oldData.keySet());

			for ( Pattern p : outputConfig.excludeColumns ) {
				for ( String key : keys ) {
					if ( p.matcher(key).matches() ) {
						this.data.remove(key);
						this.oldData.remove(key);
					}
				}
			}
		}


		EncryptionContext encryptionContext = null;
		if (outputConfig.encryptionEnabled()) {
			encryptionContext = EncryptionContext.create(outputConfig.secretKey);
		}

		DataJsonGenerator dataWriter = outputConfig.encryptionMode == EncryptionMode.ENCRYPT_DATA
			? encryptingJsonGeneratorThreadLocal.get()
			: plaintextDataGeneratorThreadLocal.get();

		JsonGenerator dataGenerator = dataWriter.begin();
		writeMapToJSON(FieldNames.DATA, this.data, dataGenerator, outputConfig.includesNulls, outputConfig.keyCase);
		if( !this.oldData.isEmpty() ){
			writeMapToJSON(FieldNames.OLD, this.oldData, dataGenerator, outputConfig.includesNulls, outputConfig.keyCase);
		}
		dataWriter.end(encryptionContext);

		g.writeEndObject(); // end of row
		g.flush();

		if(outputConfig.encryptionMode == EncryptionMode.ENCRYPT_ALL){
			String plaintext = jsonFromStream();
			encryptingJsonGeneratorThreadLocal.get().writeEncryptedObject(plaintext, encryptionContext);
			g.flush();
		}
		return jsonFromStream();
	}

	private String jsonFromStream() {
		ByteArrayOutputStream b = byteArrayThreadLocal.get();
		String s = b.toString();
		b.reset();
		return s;
	}

	public Object getData(String key) {
		return this.data.get(key);
	}

	public Object getExtraAttribute(String key) {
		return this.extraAttributes.get(key);
	}

	public long getApproximateSize() {
		return approximateSize;
	}

	private long approximateKVSize(String key, Object value) {
		long length = 0;
		length += 40; // overhead.  Whynot.
		length += key.length() * 2;

		if ( value instanceof String ) {
			length += ((String) value).length() * 2;
		} else {
			length += 64;
		}

		return length;
	}

	public void putData(String key, Object value) {
		this.data.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public void putExtraAttribute(String key, Object value) {
		if (FieldNames.isProtected(key)) {
			throw new ProtectedAttributeNameException("Extra attribute key name '" + key + "' is " +
					"a protected name. Must not be any of: " +
					String.join(", ", FieldNames.getFieldnames()));
		}
		this.extraAttributes.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public Object getOldData(String key) {
		return this.oldData.get(key);
	}

	public void putOldData(String key, Object value) {
		this.oldData.put(key, value);

		this.approximateSize += approximateKVSize(key, value);
	}

	public Position getPosition() {
		return nextPosition;
	}

	public Long getXid() {
		return xid;
	}

	public void setXid(Long xid) {
		this.xid = xid;
	}

	public void setTXCommit() {
		this.txCommit = true;
	}

	public boolean isTXCommit() {
		return this.txCommit;
	}

	public Long getServerId() {
		return serverId;
	}

	public void setServerId(Long serverId) {
		this.serverId = serverId;
	}

	public Long getThreadId() {
		return threadId;
	}

	public void setThreadId(Long threadId) {
		this.threadId = threadId;
	}

	public String getDatabase() {
		return database;
	}

	public String getTable() {
		return table;
	}

	public Long getTimestamp() {
		return timestampSeconds;
	}

	public Long getTimestampMillis() {
		return timestampMillis;
	}

	public boolean hasData(String name) {
		return this.data.containsKey(name);
	}

	public String getRowType() {
		return this.rowType;
	}

	// determines whether there is anything for the producer to output
	// override this for extended classes that don't output a value
	// return false when there is a heartbeat row or other row with suppressed output
	public boolean shouldOutput(MaxwellOutputConfig outputConfig) {
		return true;
	}

	public LinkedHashMap<String, Object> getData()
	{
		return new LinkedHashMap<>(data);
	}

	public LinkedHashMap<String, Object> getExtraAttributes()
	{
		return new LinkedHashMap<>(extraAttributes);
	}

	public LinkedHashMap<String, Object> getOldData()
	{
		return new LinkedHashMap<>(oldData);
	}
}
