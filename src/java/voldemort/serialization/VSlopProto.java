// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: slop.proto

package voldemort.serialization;

public final class VSlopProto {
  private VSlopProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface SlopOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional string store = 1;
    /**
     * <code>optional string store = 1;</code>
     */
    boolean hasStore();
    /**
     * <code>optional string store = 1;</code>
     */
    java.lang.String getStore();
    /**
     * <code>optional string store = 1;</code>
     */
    com.google.protobuf.ByteString
        getStoreBytes();

    // optional string operation = 2;
    /**
     * <code>optional string operation = 2;</code>
     */
    boolean hasOperation();
    /**
     * <code>optional string operation = 2;</code>
     */
    java.lang.String getOperation();
    /**
     * <code>optional string operation = 2;</code>
     */
    com.google.protobuf.ByteString
        getOperationBytes();

    // optional bytes key = 3;
    /**
     * <code>optional bytes key = 3;</code>
     */
    boolean hasKey();
    /**
     * <code>optional bytes key = 3;</code>
     */
    com.google.protobuf.ByteString getKey();

    // optional bytes value = 4;
    /**
     * <code>optional bytes value = 4;</code>
     */
    boolean hasValue();
    /**
     * <code>optional bytes value = 4;</code>
     */
    com.google.protobuf.ByteString getValue();

    // optional int32 node_id = 5;
    /**
     * <code>optional int32 node_id = 5;</code>
     */
    boolean hasNodeId();
    /**
     * <code>optional int32 node_id = 5;</code>
     */
    int getNodeId();

    // optional int64 arrived = 6;
    /**
     * <code>optional int64 arrived = 6;</code>
     */
    boolean hasArrived();
    /**
     * <code>optional int64 arrived = 6;</code>
     */
    long getArrived();
  }
  /**
   * Protobuf type {@code voldemort.Slop}
   */
  public static final class Slop extends
      com.google.protobuf.GeneratedMessage
      implements SlopOrBuilder {
    // Use Slop.newBuilder() to construct.
    private Slop(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Slop(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Slop defaultInstance;
    public static Slop getDefaultInstance() {
      return defaultInstance;
    }

    public Slop getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Slop(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      initFields();
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            default: {
              if (!parseUnknownField(input, unknownFields,
                                     extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
            case 10: {
              bitField0_ |= 0x00000001;
              store_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              operation_ = input.readBytes();
              break;
            }
            case 26: {
              bitField0_ |= 0x00000004;
              key_ = input.readBytes();
              break;
            }
            case 34: {
              bitField0_ |= 0x00000008;
              value_ = input.readBytes();
              break;
            }
            case 40: {
              bitField0_ |= 0x00000010;
              nodeId_ = input.readInt32();
              break;
            }
            case 48: {
              bitField0_ |= 0x00000020;
              arrived_ = input.readInt64();
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e.getMessage()).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return voldemort.serialization.VSlopProto.internal_static_voldemort_Slop_descriptor;
    }

    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return voldemort.serialization.VSlopProto.internal_static_voldemort_Slop_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              voldemort.serialization.VSlopProto.Slop.class, voldemort.serialization.VSlopProto.Slop.Builder.class);
    }

    public static com.google.protobuf.Parser<Slop> PARSER =
        new com.google.protobuf.AbstractParser<Slop>() {
      public Slop parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Slop(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<Slop> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional string store = 1;
    public static final int STORE_FIELD_NUMBER = 1;
    private java.lang.Object store_;
    /**
     * <code>optional string store = 1;</code>
     */
    public boolean hasStore() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string store = 1;</code>
     */
    public java.lang.String getStore() {
      java.lang.Object ref = store_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          store_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string store = 1;</code>
     */
    public com.google.protobuf.ByteString
        getStoreBytes() {
      java.lang.Object ref = store_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        store_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional string operation = 2;
    public static final int OPERATION_FIELD_NUMBER = 2;
    private java.lang.Object operation_;
    /**
     * <code>optional string operation = 2;</code>
     */
    public boolean hasOperation() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string operation = 2;</code>
     */
    public java.lang.String getOperation() {
      java.lang.Object ref = operation_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          operation_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string operation = 2;</code>
     */
    public com.google.protobuf.ByteString
        getOperationBytes() {
      java.lang.Object ref = operation_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        operation_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional bytes key = 3;
    public static final int KEY_FIELD_NUMBER = 3;
    private com.google.protobuf.ByteString key_;
    /**
     * <code>optional bytes key = 3;</code>
     */
    public boolean hasKey() {
      return ((bitField0_ & 0x00000004) == 0x00000004);
    }
    /**
     * <code>optional bytes key = 3;</code>
     */
    public com.google.protobuf.ByteString getKey() {
      return key_;
    }

    // optional bytes value = 4;
    public static final int VALUE_FIELD_NUMBER = 4;
    private com.google.protobuf.ByteString value_;
    /**
     * <code>optional bytes value = 4;</code>
     */
    public boolean hasValue() {
      return ((bitField0_ & 0x00000008) == 0x00000008);
    }
    /**
     * <code>optional bytes value = 4;</code>
     */
    public com.google.protobuf.ByteString getValue() {
      return value_;
    }

    // optional int32 node_id = 5;
    public static final int NODE_ID_FIELD_NUMBER = 5;
    private int nodeId_;
    /**
     * <code>optional int32 node_id = 5;</code>
     */
    public boolean hasNodeId() {
      return ((bitField0_ & 0x00000010) == 0x00000010);
    }
    /**
     * <code>optional int32 node_id = 5;</code>
     */
    public int getNodeId() {
      return nodeId_;
    }

    // optional int64 arrived = 6;
    public static final int ARRIVED_FIELD_NUMBER = 6;
    private long arrived_;
    /**
     * <code>optional int64 arrived = 6;</code>
     */
    public boolean hasArrived() {
      return ((bitField0_ & 0x00000020) == 0x00000020);
    }
    /**
     * <code>optional int64 arrived = 6;</code>
     */
    public long getArrived() {
      return arrived_;
    }

    private void initFields() {
      store_ = "";
      operation_ = "";
      key_ = com.google.protobuf.ByteString.EMPTY;
      value_ = com.google.protobuf.ByteString.EMPTY;
      nodeId_ = 0;
      arrived_ = 0L;
    }
    private byte memoizedIsInitialized = -1;
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getStoreBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getOperationBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        output.writeBytes(3, key_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        output.writeBytes(4, value_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        output.writeInt32(5, nodeId_);
      }
      if (((bitField0_ & 0x00000020) == 0x00000020)) {
        output.writeInt64(6, arrived_);
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getStoreBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getOperationBytes());
      }
      if (((bitField0_ & 0x00000004) == 0x00000004)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(3, key_);
      }
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(4, value_);
      }
      if (((bitField0_ & 0x00000010) == 0x00000010)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(5, nodeId_);
      }
      if (((bitField0_ & 0x00000020) == 0x00000020)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(6, arrived_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSerializedSize = size;
      return size;
    }

    private static final long serialVersionUID = 0L;
    @java.lang.Override
    protected java.lang.Object writeReplace()
        throws java.io.ObjectStreamException {
      return super.writeReplace();
    }

    public static voldemort.serialization.VSlopProto.Slop parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static voldemort.serialization.VSlopProto.Slop parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static voldemort.serialization.VSlopProto.Slop parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static voldemort.serialization.VSlopProto.Slop parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(voldemort.serialization.VSlopProto.Slop prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code voldemort.Slop}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements voldemort.serialization.VSlopProto.SlopOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return voldemort.serialization.VSlopProto.internal_static_voldemort_Slop_descriptor;
      }

      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return voldemort.serialization.VSlopProto.internal_static_voldemort_Slop_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                voldemort.serialization.VSlopProto.Slop.class, voldemort.serialization.VSlopProto.Slop.Builder.class);
      }

      // Construct using voldemort.serialization.VSlopProto.Slop.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage.alwaysUseFieldBuilders) {
        }
      }
      private static Builder create() {
        return new Builder();
      }

      public Builder clear() {
        super.clear();
        store_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        operation_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        key_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000004);
        value_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000008);
        nodeId_ = 0;
        bitField0_ = (bitField0_ & ~0x00000010);
        arrived_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000020);
        return this;
      }

      public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return voldemort.serialization.VSlopProto.internal_static_voldemort_Slop_descriptor;
      }

      public voldemort.serialization.VSlopProto.Slop getDefaultInstanceForType() {
        return voldemort.serialization.VSlopProto.Slop.getDefaultInstance();
      }

      public voldemort.serialization.VSlopProto.Slop build() {
        voldemort.serialization.VSlopProto.Slop result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      public voldemort.serialization.VSlopProto.Slop buildPartial() {
        voldemort.serialization.VSlopProto.Slop result = new voldemort.serialization.VSlopProto.Slop(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.store_ = store_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.operation_ = operation_;
        if (((from_bitField0_ & 0x00000004) == 0x00000004)) {
          to_bitField0_ |= 0x00000004;
        }
        result.key_ = key_;
        if (((from_bitField0_ & 0x00000008) == 0x00000008)) {
          to_bitField0_ |= 0x00000008;
        }
        result.value_ = value_;
        if (((from_bitField0_ & 0x00000010) == 0x00000010)) {
          to_bitField0_ |= 0x00000010;
        }
        result.nodeId_ = nodeId_;
        if (((from_bitField0_ & 0x00000020) == 0x00000020)) {
          to_bitField0_ |= 0x00000020;
        }
        result.arrived_ = arrived_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof voldemort.serialization.VSlopProto.Slop) {
          return mergeFrom((voldemort.serialization.VSlopProto.Slop)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(voldemort.serialization.VSlopProto.Slop other) {
        if (other == voldemort.serialization.VSlopProto.Slop.getDefaultInstance()) return this;
        if (other.hasStore()) {
          bitField0_ |= 0x00000001;
          store_ = other.store_;
          onChanged();
        }
        if (other.hasOperation()) {
          bitField0_ |= 0x00000002;
          operation_ = other.operation_;
          onChanged();
        }
        if (other.hasKey()) {
          setKey(other.getKey());
        }
        if (other.hasValue()) {
          setValue(other.getValue());
        }
        if (other.hasNodeId()) {
          setNodeId(other.getNodeId());
        }
        if (other.hasArrived()) {
          setArrived(other.getArrived());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      public final boolean isInitialized() {
        return true;
      }

      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        voldemort.serialization.VSlopProto.Slop parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (voldemort.serialization.VSlopProto.Slop) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional string store = 1;
      private java.lang.Object store_ = "";
      /**
       * <code>optional string store = 1;</code>
       */
      public boolean hasStore() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional string store = 1;</code>
       */
      public java.lang.String getStore() {
        java.lang.Object ref = store_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          store_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string store = 1;</code>
       */
      public com.google.protobuf.ByteString
          getStoreBytes() {
        java.lang.Object ref = store_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          store_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string store = 1;</code>
       */
      public Builder setStore(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        store_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string store = 1;</code>
       */
      public Builder clearStore() {
        bitField0_ = (bitField0_ & ~0x00000001);
        store_ = getDefaultInstance().getStore();
        onChanged();
        return this;
      }
      /**
       * <code>optional string store = 1;</code>
       */
      public Builder setStoreBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        store_ = value;
        onChanged();
        return this;
      }

      // optional string operation = 2;
      private java.lang.Object operation_ = "";
      /**
       * <code>optional string operation = 2;</code>
       */
      public boolean hasOperation() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string operation = 2;</code>
       */
      public java.lang.String getOperation() {
        java.lang.Object ref = operation_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          operation_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string operation = 2;</code>
       */
      public com.google.protobuf.ByteString
          getOperationBytes() {
        java.lang.Object ref = operation_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          operation_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string operation = 2;</code>
       */
      public Builder setOperation(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        operation_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string operation = 2;</code>
       */
      public Builder clearOperation() {
        bitField0_ = (bitField0_ & ~0x00000002);
        operation_ = getDefaultInstance().getOperation();
        onChanged();
        return this;
      }
      /**
       * <code>optional string operation = 2;</code>
       */
      public Builder setOperationBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        operation_ = value;
        onChanged();
        return this;
      }

      // optional bytes key = 3;
      private com.google.protobuf.ByteString key_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes key = 3;</code>
       */
      public boolean hasKey() {
        return ((bitField0_ & 0x00000004) == 0x00000004);
      }
      /**
       * <code>optional bytes key = 3;</code>
       */
      public com.google.protobuf.ByteString getKey() {
        return key_;
      }
      /**
       * <code>optional bytes key = 3;</code>
       */
      public Builder setKey(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        key_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes key = 3;</code>
       */
      public Builder clearKey() {
        bitField0_ = (bitField0_ & ~0x00000004);
        key_ = getDefaultInstance().getKey();
        onChanged();
        return this;
      }

      // optional bytes value = 4;
      private com.google.protobuf.ByteString value_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes value = 4;</code>
       */
      public boolean hasValue() {
        return ((bitField0_ & 0x00000008) == 0x00000008);
      }
      /**
       * <code>optional bytes value = 4;</code>
       */
      public com.google.protobuf.ByteString getValue() {
        return value_;
      }
      /**
       * <code>optional bytes value = 4;</code>
       */
      public Builder setValue(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes value = 4;</code>
       */
      public Builder clearValue() {
        bitField0_ = (bitField0_ & ~0x00000008);
        value_ = getDefaultInstance().getValue();
        onChanged();
        return this;
      }

      // optional int32 node_id = 5;
      private int nodeId_ ;
      /**
       * <code>optional int32 node_id = 5;</code>
       */
      public boolean hasNodeId() {
        return ((bitField0_ & 0x00000010) == 0x00000010);
      }
      /**
       * <code>optional int32 node_id = 5;</code>
       */
      public int getNodeId() {
        return nodeId_;
      }
      /**
       * <code>optional int32 node_id = 5;</code>
       */
      public Builder setNodeId(int value) {
        bitField0_ |= 0x00000010;
        nodeId_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 node_id = 5;</code>
       */
      public Builder clearNodeId() {
        bitField0_ = (bitField0_ & ~0x00000010);
        nodeId_ = 0;
        onChanged();
        return this;
      }

      // optional int64 arrived = 6;
      private long arrived_ ;
      /**
       * <code>optional int64 arrived = 6;</code>
       */
      public boolean hasArrived() {
        return ((bitField0_ & 0x00000020) == 0x00000020);
      }
      /**
       * <code>optional int64 arrived = 6;</code>
       */
      public long getArrived() {
        return arrived_;
      }
      /**
       * <code>optional int64 arrived = 6;</code>
       */
      public Builder setArrived(long value) {
        bitField0_ |= 0x00000020;
        arrived_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 arrived = 6;</code>
       */
      public Builder clearArrived() {
        bitField0_ = (bitField0_ & ~0x00000020);
        arrived_ = 0L;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:voldemort.Slop)
    }

    static {
      defaultInstance = new Slop(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:voldemort.Slop)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_voldemort_Slop_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_voldemort_Slop_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\nslop.proto\022\tvoldemort\"f\n\004Slop\022\r\n\005store" +
      "\030\001 \001(\t\022\021\n\toperation\030\002 \001(\t\022\013\n\003key\030\003 \001(\014\022\r" +
      "\n\005value\030\004 \001(\014\022\017\n\007node_id\030\005 \001(\005\022\017\n\007arrive" +
      "d\030\006 \001(\003B\'\n\027voldemort.serializationB\nVSlo" +
      "pProtoH\001"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_voldemort_Slop_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_voldemort_Slop_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_voldemort_Slop_descriptor,
              new java.lang.String[] { "Store", "Operation", "Key", "Value", "NodeId", "Arrived", });
          return null;
        }
      };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
