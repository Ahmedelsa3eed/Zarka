/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.zarka.avro;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class Weather extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = 2262693807853532245L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Weather\",\"namespace\":\"org.zarka.avro\",\"fields\":[{\"name\":\"humidity\",\"type\":\"int\"},{\"name\":\"temperature\",\"type\":\"int\"},{\"name\":\"wind_speed\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<Weather> ENCODER =
      new BinaryMessageEncoder<>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Weather> DECODER =
      new BinaryMessageDecoder<>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Weather> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Weather> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Weather> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Weather to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Weather from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Weather instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Weather fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private int humidity;
  private int temperature;
  private int wind_speed;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Weather() {}

  /**
   * All-args constructor.
   * @param humidity The new value for humidity
   * @param temperature The new value for temperature
   * @param wind_speed The new value for wind_speed
   */
  public Weather(java.lang.Integer humidity, java.lang.Integer temperature, java.lang.Integer wind_speed) {
    this.humidity = humidity;
    this.temperature = temperature;
    this.wind_speed = wind_speed;
  }

  @Override
  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }

  @Override
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  // Used by DatumWriter.  Applications should not call.
  @Override
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return humidity;
    case 1: return temperature;
    case 2: return wind_speed;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @Override
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: humidity = (java.lang.Integer)value$; break;
    case 1: temperature = (java.lang.Integer)value$; break;
    case 2: wind_speed = (java.lang.Integer)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'humidity' field.
   * @return The value of the 'humidity' field.
   */
  public int getHumidity() {
    return humidity;
  }


  /**
   * Sets the value of the 'humidity' field.
   * @param value the value to set.
   */
  public void setHumidity(int value) {
    this.humidity = value;
  }

  /**
   * Gets the value of the 'temperature' field.
   * @return The value of the 'temperature' field.
   */
  public int getTemperature() {
    return temperature;
  }


  /**
   * Sets the value of the 'temperature' field.
   * @param value the value to set.
   */
  public void setTemperature(int value) {
    this.temperature = value;
  }

  /**
   * Gets the value of the 'wind_speed' field.
   * @return The value of the 'wind_speed' field.
   */
  public int getWindSpeed() {
    return wind_speed;
  }


  /**
   * Sets the value of the 'wind_speed' field.
   * @param value the value to set.
   */
  public void setWindSpeed(int value) {
    this.wind_speed = value;
  }

  /**
   * Creates a new Weather RecordBuilder.
   * @return A new Weather RecordBuilder
   */
  public static org.zarka.avro.Weather.Builder newBuilder() {
    return new org.zarka.avro.Weather.Builder();
  }

  /**
   * Creates a new Weather RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Weather RecordBuilder
   */
  public static org.zarka.avro.Weather.Builder newBuilder(org.zarka.avro.Weather.Builder other) {
    if (other == null) {
      return new org.zarka.avro.Weather.Builder();
    } else {
      return new org.zarka.avro.Weather.Builder(other);
    }
  }

  /**
   * Creates a new Weather RecordBuilder by copying an existing Weather instance.
   * @param other The existing instance to copy.
   * @return A new Weather RecordBuilder
   */
  public static org.zarka.avro.Weather.Builder newBuilder(org.zarka.avro.Weather other) {
    if (other == null) {
      return new org.zarka.avro.Weather.Builder();
    } else {
      return new org.zarka.avro.Weather.Builder(other);
    }
  }

  /**
   * RecordBuilder for Weather instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Weather>
    implements org.apache.avro.data.RecordBuilder<Weather> {

    private int humidity;
    private int temperature;
    private int wind_speed;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.zarka.avro.Weather.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.humidity)) {
        this.humidity = data().deepCopy(fields()[0].schema(), other.humidity);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.temperature)) {
        this.temperature = data().deepCopy(fields()[1].schema(), other.temperature);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.wind_speed)) {
        this.wind_speed = data().deepCopy(fields()[2].schema(), other.wind_speed);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
    }

    /**
     * Creates a Builder by copying an existing Weather instance
     * @param other The existing instance to copy.
     */
    private Builder(org.zarka.avro.Weather other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.humidity)) {
        this.humidity = data().deepCopy(fields()[0].schema(), other.humidity);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.temperature)) {
        this.temperature = data().deepCopy(fields()[1].schema(), other.temperature);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.wind_speed)) {
        this.wind_speed = data().deepCopy(fields()[2].schema(), other.wind_speed);
        fieldSetFlags()[2] = true;
      }
    }

    /**
      * Gets the value of the 'humidity' field.
      * @return The value.
      */
    public int getHumidity() {
      return humidity;
    }


    /**
      * Sets the value of the 'humidity' field.
      * @param value The value of 'humidity'.
      * @return This builder.
      */
    public org.zarka.avro.Weather.Builder setHumidity(int value) {
      validate(fields()[0], value);
      this.humidity = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'humidity' field has been set.
      * @return True if the 'humidity' field has been set, false otherwise.
      */
    public boolean hasHumidity() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'humidity' field.
      * @return This builder.
      */
    public org.zarka.avro.Weather.Builder clearHumidity() {
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'temperature' field.
      * @return The value.
      */
    public int getTemperature() {
      return temperature;
    }


    /**
      * Sets the value of the 'temperature' field.
      * @param value The value of 'temperature'.
      * @return This builder.
      */
    public org.zarka.avro.Weather.Builder setTemperature(int value) {
      validate(fields()[1], value);
      this.temperature = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'temperature' field has been set.
      * @return True if the 'temperature' field has been set, false otherwise.
      */
    public boolean hasTemperature() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'temperature' field.
      * @return This builder.
      */
    public org.zarka.avro.Weather.Builder clearTemperature() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'wind_speed' field.
      * @return The value.
      */
    public int getWindSpeed() {
      return wind_speed;
    }


    /**
      * Sets the value of the 'wind_speed' field.
      * @param value The value of 'wind_speed'.
      * @return This builder.
      */
    public org.zarka.avro.Weather.Builder setWindSpeed(int value) {
      validate(fields()[2], value);
      this.wind_speed = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'wind_speed' field has been set.
      * @return True if the 'wind_speed' field has been set, false otherwise.
      */
    public boolean hasWindSpeed() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'wind_speed' field.
      * @return This builder.
      */
    public org.zarka.avro.Weather.Builder clearWindSpeed() {
      fieldSetFlags()[2] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Weather build() {
      try {
        Weather record = new Weather();
        record.humidity = fieldSetFlags()[0] ? this.humidity : (java.lang.Integer) defaultValue(fields()[0]);
        record.temperature = fieldSetFlags()[1] ? this.temperature : (java.lang.Integer) defaultValue(fields()[1]);
        record.wind_speed = fieldSetFlags()[2] ? this.wind_speed : (java.lang.Integer) defaultValue(fields()[2]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Weather>
    WRITER$ = (org.apache.avro.io.DatumWriter<Weather>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Weather>
    READER$ = (org.apache.avro.io.DatumReader<Weather>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeInt(this.humidity);

    out.writeInt(this.temperature);

    out.writeInt(this.wind_speed);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.humidity = in.readInt();

      this.temperature = in.readInt();

      this.wind_speed = in.readInt();

    } else {
      for (int i = 0; i < 3; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.humidity = in.readInt();
          break;

        case 1:
          this.temperature = in.readInt();
          break;

        case 2:
          this.wind_speed = in.readInt();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










