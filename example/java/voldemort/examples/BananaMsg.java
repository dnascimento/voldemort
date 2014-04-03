// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: bananaMsg.proto

package voldemort.examples;

public final class BananaMsg {
  private BananaMsg() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
  }
  public interface BananaOrBuilder
      extends com.google.protobuf.MessageOrBuilder {

    // optional string nome = 1;
    /**
     * <code>optional string nome = 1;</code>
     */
    boolean hasNome();
    /**
     * <code>optional string nome = 1;</code>
     */
    java.lang.String getNome();
    /**
     * <code>optional string nome = 1;</code>
     */
    com.google.protobuf.ByteString
        getNomeBytes();

    // optional string tipo = 2;
    /**
     * <code>optional string tipo = 2;</code>
     */
    boolean hasTipo();
    /**
     * <code>optional string tipo = 2;</code>
     */
    java.lang.String getTipo();
    /**
     * <code>optional string tipo = 2;</code>
     */
    com.google.protobuf.ByteString
        getTipoBytes();
  }
  /**
   * Protobuf type {@code voldemort.examples.Banana}
   */
  public static final class Banana extends
      com.google.protobuf.GeneratedMessage
      implements BananaOrBuilder {
    // Use Banana.newBuilder() to construct.
    private Banana(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
      this.unknownFields = builder.getUnknownFields();
    }
    private Banana(boolean noInit) { this.unknownFields = com.google.protobuf.UnknownFieldSet.getDefaultInstance(); }

    private static final Banana defaultInstance;
    public static Banana getDefaultInstance() {
      return defaultInstance;
    }

    @Override
    public Banana getDefaultInstanceForType() {
      return defaultInstance;
    }

    private final com.google.protobuf.UnknownFieldSet unknownFields;
    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
        getUnknownFields() {
      return this.unknownFields;
    }
    private Banana(
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
              nome_ = input.readBytes();
              break;
            }
            case 18: {
              bitField0_ |= 0x00000002;
              tipo_ = input.readBytes();
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
      return voldemort.examples.BananaMsg.internal_static_voldemort_examples_Banana_descriptor;
    }

    @Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return voldemort.examples.BananaMsg.internal_static_voldemort_examples_Banana_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              voldemort.examples.BananaMsg.Banana.class, voldemort.examples.BananaMsg.Banana.Builder.class);
    }

    public static com.google.protobuf.Parser<Banana> PARSER =
        new com.google.protobuf.AbstractParser<Banana>() {
      @Override
    public Banana parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new Banana(input, extensionRegistry);
      }
    };

    @java.lang.Override
    public com.google.protobuf.Parser<Banana> getParserForType() {
      return PARSER;
    }

    private int bitField0_;
    // optional string nome = 1;
    public static final int NOME_FIELD_NUMBER = 1;
    private java.lang.Object nome_;
    /**
     * <code>optional string nome = 1;</code>
     */
    @Override
    public boolean hasNome() {
      return ((bitField0_ & 0x00000001) == 0x00000001);
    }
    /**
     * <code>optional string nome = 1;</code>
     */
    @Override
    public java.lang.String getNome() {
      java.lang.Object ref = nome_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          nome_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string nome = 1;</code>
     */
    @Override
    public com.google.protobuf.ByteString
        getNomeBytes() {
      java.lang.Object ref = nome_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        nome_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    // optional string tipo = 2;
    public static final int TIPO_FIELD_NUMBER = 2;
    private java.lang.Object tipo_;
    /**
     * <code>optional string tipo = 2;</code>
     */
    @Override
    public boolean hasTipo() {
      return ((bitField0_ & 0x00000002) == 0x00000002);
    }
    /**
     * <code>optional string tipo = 2;</code>
     */
    @Override
    public java.lang.String getTipo() {
      java.lang.Object ref = tipo_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          tipo_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string tipo = 2;</code>
     */
    @Override
    public com.google.protobuf.ByteString
        getTipoBytes() {
      java.lang.Object ref = tipo_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tipo_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private void initFields() {
      nome_ = "";
      tipo_ = "";
    }
    private byte memoizedIsInitialized = -1;
    @Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized != -1) return isInitialized == 1;

      memoizedIsInitialized = 1;
      return true;
    }

    @Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      getSerializedSize();
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        output.writeBytes(1, getNomeBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        output.writeBytes(2, getTipoBytes());
      }
      getUnknownFields().writeTo(output);
    }

    private int memoizedSerializedSize = -1;
    @Override
    public int getSerializedSize() {
      int size = memoizedSerializedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(1, getNomeBytes());
      }
      if (((bitField0_ & 0x00000002) == 0x00000002)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(2, getTipoBytes());
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

    public static voldemort.examples.BananaMsg.Banana parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }
    public static voldemort.examples.BananaMsg.Banana parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input);
    }
    public static voldemort.examples.BananaMsg.Banana parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseDelimitedFrom(input, extensionRegistry);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return PARSER.parseFrom(input);
    }
    public static voldemort.examples.BananaMsg.Banana parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return PARSER.parseFrom(input, extensionRegistry);
    }

    public static Builder newBuilder() { return Builder.create(); }
    @Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder(voldemort.examples.BananaMsg.Banana prototype) {
      return newBuilder().mergeFrom(prototype);
    }
    @Override
    public Builder toBuilder() { return newBuilder(this); }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code voldemort.examples.Banana}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder>
       implements voldemort.examples.BananaMsg.BananaOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return voldemort.examples.BananaMsg.internal_static_voldemort_examples_Banana_descriptor;
      }

      @Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return voldemort.examples.BananaMsg.internal_static_voldemort_examples_Banana_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                voldemort.examples.BananaMsg.Banana.class, voldemort.examples.BananaMsg.Banana.Builder.class);
      }

      // Construct using voldemort.examples.BananaMsg.Banana.newBuilder()
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

      @Override
    public Builder clear() {
        super.clear();
        nome_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        tipo_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @Override
    public Builder clone() {
        return create().mergeFrom(buildPartial());
      }

      @Override
    public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return voldemort.examples.BananaMsg.internal_static_voldemort_examples_Banana_descriptor;
      }

      @Override
    public voldemort.examples.BananaMsg.Banana getDefaultInstanceForType() {
        return voldemort.examples.BananaMsg.Banana.getDefaultInstance();
      }

      @Override
    public voldemort.examples.BananaMsg.Banana build() {
        voldemort.examples.BananaMsg.Banana result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @Override
    public voldemort.examples.BananaMsg.Banana buildPartial() {
        voldemort.examples.BananaMsg.Banana result = new voldemort.examples.BananaMsg.Banana(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) == 0x00000001)) {
          to_bitField0_ |= 0x00000001;
        }
        result.nome_ = nome_;
        if (((from_bitField0_ & 0x00000002) == 0x00000002)) {
          to_bitField0_ |= 0x00000002;
        }
        result.tipo_ = tipo_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof voldemort.examples.BananaMsg.Banana) {
          return mergeFrom((voldemort.examples.BananaMsg.Banana)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(voldemort.examples.BananaMsg.Banana other) {
        if (other == voldemort.examples.BananaMsg.Banana.getDefaultInstance()) return this;
        if (other.hasNome()) {
          bitField0_ |= 0x00000001;
          nome_ = other.nome_;
          onChanged();
        }
        if (other.hasTipo()) {
          bitField0_ |= 0x00000002;
          tipo_ = other.tipo_;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        return this;
      }

      @Override
    public final boolean isInitialized() {
        return true;
      }

      @Override
    public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        voldemort.examples.BananaMsg.Banana parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (voldemort.examples.BananaMsg.Banana) e.getUnfinishedMessage();
          throw e;
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      // optional string nome = 1;
      private java.lang.Object nome_ = "";
      /**
       * <code>optional string nome = 1;</code>
       */
      @Override
    public boolean hasNome() {
        return ((bitField0_ & 0x00000001) == 0x00000001);
      }
      /**
       * <code>optional string nome = 1;</code>
       */
      @Override
    public java.lang.String getNome() {
        java.lang.Object ref = nome_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          nome_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string nome = 1;</code>
       */
      @Override
    public com.google.protobuf.ByteString
          getNomeBytes() {
        java.lang.Object ref = nome_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          nome_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string nome = 1;</code>
       */
      public Builder setNome(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        nome_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string nome = 1;</code>
       */
      public Builder clearNome() {
        bitField0_ = (bitField0_ & ~0x00000001);
        nome_ = getDefaultInstance().getNome();
        onChanged();
        return this;
      }
      /**
       * <code>optional string nome = 1;</code>
       */
      public Builder setNomeBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        nome_ = value;
        onChanged();
        return this;
      }

      // optional string tipo = 2;
      private java.lang.Object tipo_ = "";
      /**
       * <code>optional string tipo = 2;</code>
       */
      @Override
    public boolean hasTipo() {
        return ((bitField0_ & 0x00000002) == 0x00000002);
      }
      /**
       * <code>optional string tipo = 2;</code>
       */
      @Override
    public java.lang.String getTipo() {
        java.lang.Object ref = tipo_;
        if (!(ref instanceof java.lang.String)) {
          java.lang.String s = ((com.google.protobuf.ByteString) ref)
              .toStringUtf8();
          tipo_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string tipo = 2;</code>
       */
      @Override
    public com.google.protobuf.ByteString
          getTipoBytes() {
        java.lang.Object ref = tipo_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          tipo_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string tipo = 2;</code>
       */
      public Builder setTipo(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        tipo_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string tipo = 2;</code>
       */
      public Builder clearTipo() {
        bitField0_ = (bitField0_ & ~0x00000002);
        tipo_ = getDefaultInstance().getTipo();
        onChanged();
        return this;
      }
      /**
       * <code>optional string tipo = 2;</code>
       */
      public Builder setTipoBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        tipo_ = value;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:voldemort.examples.Banana)
    }

    static {
      defaultInstance = new Banana(true);
      defaultInstance.initFields();
    }

    // @@protoc_insertion_point(class_scope:voldemort.examples.Banana)
  }

  private static com.google.protobuf.Descriptors.Descriptor
    internal_static_voldemort_examples_Banana_descriptor;
  private static
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_voldemort_examples_Banana_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\017bananaMsg.proto\022\022voldemort.examples\"$\n" +
      "\006Banana\022\014\n\004nome\030\001 \001(\t\022\014\n\004tipo\030\002 \001(\t"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
      new com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner() {
        @Override
        public com.google.protobuf.ExtensionRegistry assignDescriptors(
            com.google.protobuf.Descriptors.FileDescriptor root) {
          descriptor = root;
          internal_static_voldemort_examples_Banana_descriptor =
            getDescriptor().getMessageTypes().get(0);
          internal_static_voldemort_examples_Banana_fieldAccessorTable = new
            com.google.protobuf.GeneratedMessage.FieldAccessorTable(
              internal_static_voldemort_examples_Banana_descriptor,
              new java.lang.String[] { "Nome", "Tipo", });
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
