/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.io;

import com.fasterxml.jackson.core.Base64Variant;
import com.fasterxml.jackson.core.FormatSchema;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.SerializableString;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * A wrapper class for an existing Json parser that suppresses parse errors and just continue parsing.
 * This is useful for parsing ill-formatted Json files or for starting the parser at the middle of the file.
 */
public class SilentJsonParser extends JsonParser {
  private static final Log LOG = LogFactory.getLog(SilentJsonParser.class);

  /** A flag that is raised when the end of file is reached */
  protected boolean eofReached = false;

  @Override
  public ObjectCodec getCodec() {
    return wrapped.getCodec();
  }

  @Override
  public void setCodec(ObjectCodec objectCodec) {
    wrapped.setCodec(objectCodec);
  }

  @Override
  public Object getInputSource() {
    return wrapped.getInputSource();
  }

  @Override
  public Object getCurrentValue() {
    return wrapped.getCurrentValue();
  }

  @Override
  public void setCurrentValue(Object v) {
    wrapped.setCurrentValue(v);
  }

  @Override
  public void setSchema(FormatSchema schema) {
    wrapped.setSchema(schema);
  }

  @Override
  public FormatSchema getSchema() {
    return wrapped.getSchema();
  }

  @Override
  public boolean canUseSchema(FormatSchema schema) {
    return wrapped.canUseSchema(schema);
  }

  @Override
  public boolean requiresCustomCodec() {
    return wrapped.requiresCustomCodec();
  }

  @Override
  public Version version() {
    return wrapped.version();
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }

  @Override
  public boolean isClosed() {
    return wrapped.isClosed();
  }

  @Override
  public JsonStreamContext getParsingContext() {
    return wrapped.getParsingContext();
  }

  @Override
  public JsonLocation getTokenLocation() {
    return wrapped.getTokenLocation();
  }

  @Override
  public JsonLocation getCurrentLocation() {
    return wrapped.getCurrentLocation();
  }

  @Override
  public int releaseBuffered(OutputStream out) throws IOException {
    return wrapped.releaseBuffered(out);
  }

  @Override
  public int releaseBuffered(Writer w) throws IOException {
    return wrapped.releaseBuffered(w);
  }

  @Override
  public JsonParser enable(Feature f) {
    return wrapped.enable(f);
  }

  @Override
  public JsonParser disable(Feature f) {
    return wrapped.disable(f);
  }

  @Override
  public JsonParser configure(Feature f, boolean state) {
    return wrapped.configure(f, state);
  }

  @Override
  public boolean isEnabled(Feature f) {
    return wrapped.isEnabled(f);
  }

  @Override
  public int getFeatureMask() {
    return wrapped.getFeatureMask();
  }

  @Override
  @Deprecated
  public JsonParser setFeatureMask(int mask) {
    return wrapped.setFeatureMask(mask);
  }

  @Override
  public JsonParser overrideStdFeatures(int values, int mask) {
    return wrapped.overrideStdFeatures(values, mask);
  }

  @Override
  public int getFormatFeatures() {
    return wrapped.getFormatFeatures();
  }

  @Override
  public JsonParser overrideFormatFeatures(int values, int mask) {
    return wrapped.overrideFormatFeatures(values, mask);
  }

  @Override
  public JsonToken nextToken() throws IOException {
    do {
      try {
        return wrapped.nextToken();
      } catch (JsonParseException e) {
        // Suppress the error and continue;
        eofReached = e instanceof JsonEOFException;
      }
    } while (!eofReached);
    return JsonToken.NOT_AVAILABLE;
  }

  @Override
  public JsonToken nextValue() throws IOException {
    return wrapped.nextValue();
  }

  @Override
  public boolean nextFieldName(SerializableString str) throws IOException {
    return wrapped.nextFieldName(str);
  }

  @Override
  public String nextFieldName() throws IOException {
    return wrapped.nextFieldName();
  }

  @Override
  public String nextTextValue() throws IOException {
    return wrapped.nextTextValue();
  }

  @Override
  public int nextIntValue(int defaultValue) throws IOException {
    return wrapped.nextIntValue(defaultValue);
  }

  @Override
  public long nextLongValue(long defaultValue) throws IOException {
    return wrapped.nextLongValue(defaultValue);
  }

  @Override
  public Boolean nextBooleanValue() throws IOException {
    return wrapped.nextBooleanValue();
  }

  @Override
  public JsonParser skipChildren() throws IOException {
    return wrapped.skipChildren();
  }

  @Override
  public JsonToken getCurrentToken() {
    return eofReached? JsonToken.NOT_AVAILABLE : wrapped.getCurrentToken();
  }

  @Override
  public int getCurrentTokenId() {
    return wrapped.getCurrentTokenId();
  }

  @Override
  public boolean hasCurrentToken() {
    return wrapped.hasCurrentToken();
  }

  @Override
  public boolean hasTokenId(int i) {
    return wrapped.hasTokenId(i);
  }

  @Override
  public boolean hasToken(JsonToken jsonToken) {
    return wrapped.hasToken(jsonToken);
  }

  @Override
  public boolean isExpectedStartArrayToken() {
    return wrapped.isExpectedStartArrayToken();
  }

  @Override
  public boolean isExpectedStartObjectToken() {
    return wrapped.isExpectedStartObjectToken();
  }

  @Override
  public void clearCurrentToken() {
    wrapped.clearCurrentToken();
  }

  @Override
  public JsonToken getLastClearedToken() {
    return wrapped.getLastClearedToken();
  }

  @Override
  public void overrideCurrentName(String s) {
    wrapped.overrideCurrentName(s);
  }

  @Override
  public String getCurrentName() throws IOException {
    return wrapped.getCurrentName();
  }

  @Override
  public String getText() throws IOException {
    return wrapped.getText();
  }

  @Override
  public char[] getTextCharacters() throws IOException {
    return wrapped.getTextCharacters();
  }

  @Override
  public int getTextLength() throws IOException {
    return wrapped.getTextLength();
  }

  @Override
  public int getTextOffset() throws IOException {
    return wrapped.getTextOffset();
  }

  @Override
  public boolean hasTextCharacters() {
    return wrapped.hasTextCharacters();
  }

  @Override
  public Number getNumberValue() throws IOException {
    return wrapped.getNumberValue();
  }

  @Override
  public NumberType getNumberType() throws IOException {
    return wrapped.getNumberType();
  }

  @Override
  public byte getByteValue() throws IOException {
    return wrapped.getByteValue();
  }

  @Override
  public short getShortValue() throws IOException {
    return wrapped.getShortValue();
  }

  @Override
  public int getIntValue() throws IOException {
    try {
      return wrapped.getIntValue();
    } catch (com.fasterxml.jackson.core.exc.InputCoercionException e) {
      LOG.warn("Value too big", e);
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public long getLongValue() throws IOException {
    return wrapped.getLongValue();
  }

  @Override
  public BigInteger getBigIntegerValue() throws IOException {
    return wrapped.getBigIntegerValue();
  }

  @Override
  public float getFloatValue() throws IOException {
    return wrapped.getFloatValue();
  }

  @Override
  public double getDoubleValue() throws IOException {
    return wrapped.getDoubleValue();
  }

  @Override
  public BigDecimal getDecimalValue() throws IOException {
    return wrapped.getDecimalValue();
  }

  @Override
  public boolean getBooleanValue() throws IOException {
    return wrapped.getBooleanValue();
  }

  @Override
  public Object getEmbeddedObject() throws IOException {
    return wrapped.getEmbeddedObject();
  }

  @Override
  public byte[] getBinaryValue(Base64Variant base64Variant) throws IOException {
    return wrapped.getBinaryValue(base64Variant);
  }

  @Override
  public byte[] getBinaryValue() throws IOException {
    return wrapped.getBinaryValue();
  }

  @Override
  public int readBinaryValue(OutputStream out) throws IOException {
    return wrapped.readBinaryValue(out);
  }

  @Override
  public int readBinaryValue(Base64Variant bv, OutputStream out) throws IOException {
    return wrapped.readBinaryValue(bv, out);
  }

  @Override
  public int getValueAsInt() throws IOException {
    return wrapped.getValueAsInt();
  }

  @Override
  public int getValueAsInt(int def) throws IOException {
    return wrapped.getValueAsInt(def);
  }

  @Override
  public long getValueAsLong() throws IOException {
    return wrapped.getValueAsLong();
  }

  @Override
  public long getValueAsLong(long def) throws IOException {
    return wrapped.getValueAsLong(def);
  }

  @Override
  public double getValueAsDouble() throws IOException {
    return wrapped.getValueAsDouble();
  }

  @Override
  public double getValueAsDouble(double def) throws IOException {
    return wrapped.getValueAsDouble(def);
  }

  @Override
  public boolean getValueAsBoolean() throws IOException {
    return wrapped.getValueAsBoolean();
  }

  @Override
  public boolean getValueAsBoolean(boolean def) throws IOException {
    return wrapped.getValueAsBoolean(def);
  }

  @Override
  public String getValueAsString() throws IOException {
    return wrapped.getValueAsString();
  }

  @Override
  public String getValueAsString(String s) throws IOException {
    return wrapped.getValueAsString(s);
  }

  @Override
  public boolean canReadObjectId() {
    return wrapped.canReadObjectId();
  }

  @Override
  public boolean canReadTypeId() {
    return wrapped.canReadTypeId();
  }

  @Override
  public Object getObjectId() throws IOException {
    return wrapped.getObjectId();
  }

  @Override
  public Object getTypeId() throws IOException {
    return wrapped.getTypeId();
  }

  @Override
  public <T> T readValueAs(Class<T> valueType) throws IOException {
    return wrapped.readValueAs(valueType);
  }

  @Override
  public <T> T readValueAs(TypeReference<?> valueTypeRef) throws IOException {
    return wrapped.readValueAs(valueTypeRef);
  }

  @Override
  public <T> Iterator<T> readValuesAs(Class<T> valueType) throws IOException {
    return wrapped.readValuesAs(valueType);
  }

  @Override
  public <T extends TreeNode> T readValueAsTree() throws IOException {
    return wrapped.readValueAsTree();
  }

  @Override
  public ObjectCodec _codec() {
    throw new RuntimeException("Should never get called");
  }

  @Override
  public JsonParseException _constructError(String msg) {
    throw new RuntimeException("Should never get called");
  }

  @Override
  public void _reportUnsupportedOperation() {
    throw new RuntimeException("Should never get called");
  }

  /**The wrapped JsonParser*/
  private final JsonParser wrapped;

  public SilentJsonParser(JsonParser wrapped) {
    this.wrapped = wrapped;
  }
}
