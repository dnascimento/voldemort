# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: voldemort-client.proto

from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)




DESCRIPTOR = _descriptor.FileDescriptor(
  name='voldemort-client.proto',
  package='voldemort',
  serialized_pb='\n\x16voldemort-client.proto\x12\tvoldemort\".\n\nClockEntry\x12\x0f\n\x07node_id\x18\x01 \x02(\x05\x12\x0f\n\x07version\x18\x02 \x02(\x03\"H\n\x0bVectorClock\x12&\n\x07\x65ntries\x18\x01 \x03(\x0b\x32\x15.voldemort.ClockEntry\x12\x11\n\ttimestamp\x18\x02 \x01(\x03\"C\n\tVersioned\x12\r\n\x05value\x18\x01 \x02(\x0c\x12\'\n\x07version\x18\x02 \x02(\x0b\x32\x16.voldemort.VectorClock\"2\n\x05\x45rror\x12\x12\n\nerror_code\x18\x01 \x02(\x05\x12\x15\n\rerror_message\x18\x02 \x02(\t\"Q\n\rKeyedVersions\x12\x0b\n\x03key\x18\x01 \x02(\x0c\x12&\n\x08versions\x18\x02 \x03(\x0b\x32\x14.voldemort.Versioned\x12\x0b\n\x03rid\x18\x03 \x01(\x03\":\n\nGetRequest\x12\x0b\n\x03key\x18\x01 \x01(\x0c\x12\x12\n\ntransforms\x18\x02 \x01(\x0c\x12\x0b\n\x03rid\x18\x03 \x01(\x03\"d\n\x0bGetResponse\x12\'\n\tversioned\x18\x01 \x03(\x0b\x32\x14.voldemort.Versioned\x12\x1f\n\x05\x65rror\x18\x02 \x01(\x0b\x32\x10.voldemort.Error\x12\x0b\n\x03rid\x18\x03 \x01(\x03\"l\n\x12GetVersionResponse\x12(\n\x08versions\x18\x01 \x03(\x0b\x32\x16.voldemort.VectorClock\x12\x1f\n\x05\x65rror\x18\x02 \x01(\x0b\x32\x10.voldemort.Error\x12\x0b\n\x03rid\x18\x03 \x01(\x03\"\x9b\x01\n\rGetAllRequest\x12\x0c\n\x04keys\x18\x01 \x03(\x0c\x12<\n\ntransforms\x18\x02 \x03(\x0b\x32(.voldemort.GetAllRequest.GetAllTransform\x12\x0b\n\x03rid\x18\x03 \x01(\x03\x1a\x31\n\x0fGetAllTransform\x12\x0b\n\x03key\x18\x01 \x02(\x0c\x12\x11\n\ttransform\x18\x02 \x02(\x0c\"[\n\x0eGetAllResponse\x12(\n\x06values\x18\x01 \x03(\x0b\x32\x18.voldemort.KeyedVersions\x12\x1f\n\x05\x65rror\x18\x02 \x01(\x0b\x32\x10.voldemort.Error\"c\n\nPutRequest\x12\x0b\n\x03key\x18\x01 \x02(\x0c\x12\'\n\tversioned\x18\x02 \x02(\x0b\x32\x14.voldemort.Versioned\x12\x12\n\ntransforms\x18\x03 \x01(\x0c\x12\x0b\n\x03rid\x18\x04 \x01(\x03\".\n\x0bPutResponse\x12\x1f\n\x05\x65rror\x18\x01 \x01(\x0b\x32\x10.voldemort.Error\"R\n\rDeleteRequest\x12\x0b\n\x03key\x18\x01 \x02(\x0c\x12\'\n\x07version\x18\x02 \x02(\x0b\x32\x16.voldemort.VectorClock\x12\x0b\n\x03rid\x18\x03 \x01(\x03\"B\n\x0e\x44\x65leteResponse\x12\x0f\n\x07success\x18\x01 \x02(\x08\x12\x1f\n\x05\x65rror\x18\x02 \x01(\x0b\x32\x10.voldemort.Error\"\x9a\x02\n\x10VoldemortRequest\x12$\n\x04type\x18\x01 \x02(\x0e\x32\x16.voldemort.RequestType\x12\x1b\n\x0cshould_route\x18\x02 \x02(\x08:\x05\x66\x61lse\x12\r\n\x05store\x18\x03 \x02(\t\x12\"\n\x03get\x18\x04 \x01(\x0b\x32\x15.voldemort.GetRequest\x12(\n\x06getAll\x18\x05 \x01(\x0b\x32\x18.voldemort.GetAllRequest\x12\"\n\x03put\x18\x06 \x01(\x0b\x32\x15.voldemort.PutRequest\x12(\n\x06\x64\x65lete\x18\x07 \x01(\x0b\x32\x18.voldemort.DeleteRequest\x12\x18\n\x10requestRouteType\x18\x08 \x01(\x05*I\n\x0bRequestType\x12\x07\n\x03GET\x10\x00\x12\x0b\n\x07GET_ALL\x10\x01\x12\x07\n\x03PUT\x10\x02\x12\n\n\x06\x44\x45LETE\x10\x03\x12\x0f\n\x0bGET_VERSION\x10\x04\x42(\n\x1cvoldemort.client.protocol.pbB\x06VProtoH\x01')

_REQUESTTYPE = _descriptor.EnumDescriptor(
  name='RequestType',
  full_name='voldemort.RequestType',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='GET', index=0, number=0,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='GET_ALL', index=1, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='PUT', index=2, number=2,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='DELETE', index=3, number=3,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='GET_VERSION', index=4, number=4,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=1472,
  serialized_end=1545,
)

RequestType = enum_type_wrapper.EnumTypeWrapper(_REQUESTTYPE)
GET = 0
GET_ALL = 1
PUT = 2
DELETE = 3
GET_VERSION = 4



_CLOCKENTRY = _descriptor.Descriptor(
  name='ClockEntry',
  full_name='voldemort.ClockEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='node_id', full_name='voldemort.ClockEntry.node_id', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='version', full_name='voldemort.ClockEntry.version', index=1,
      number=2, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=37,
  serialized_end=83,
)


_VECTORCLOCK = _descriptor.Descriptor(
  name='VectorClock',
  full_name='voldemort.VectorClock',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='entries', full_name='voldemort.VectorClock.entries', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='timestamp', full_name='voldemort.VectorClock.timestamp', index=1,
      number=2, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=85,
  serialized_end=157,
)


_VERSIONED = _descriptor.Descriptor(
  name='Versioned',
  full_name='voldemort.Versioned',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='value', full_name='voldemort.Versioned.value', index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='version', full_name='voldemort.Versioned.version', index=1,
      number=2, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=159,
  serialized_end=226,
)


_ERROR = _descriptor.Descriptor(
  name='Error',
  full_name='voldemort.Error',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='error_code', full_name='voldemort.Error.error_code', index=0,
      number=1, type=5, cpp_type=1, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error_message', full_name='voldemort.Error.error_message', index=1,
      number=2, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=228,
  serialized_end=278,
)


_KEYEDVERSIONS = _descriptor.Descriptor(
  name='KeyedVersions',
  full_name='voldemort.KeyedVersions',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='voldemort.KeyedVersions.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='versions', full_name='voldemort.KeyedVersions.versions', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.KeyedVersions.rid', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=280,
  serialized_end=361,
)


_GETREQUEST = _descriptor.Descriptor(
  name='GetRequest',
  full_name='voldemort.GetRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='voldemort.GetRequest.key', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='transforms', full_name='voldemort.GetRequest.transforms', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.GetRequest.rid', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=363,
  serialized_end=421,
)


_GETRESPONSE = _descriptor.Descriptor(
  name='GetResponse',
  full_name='voldemort.GetResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='versioned', full_name='voldemort.GetResponse.versioned', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error', full_name='voldemort.GetResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.GetResponse.rid', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=423,
  serialized_end=523,
)


_GETVERSIONRESPONSE = _descriptor.Descriptor(
  name='GetVersionResponse',
  full_name='voldemort.GetVersionResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='versions', full_name='voldemort.GetVersionResponse.versions', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error', full_name='voldemort.GetVersionResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.GetVersionResponse.rid', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=525,
  serialized_end=633,
)


_GETALLREQUEST_GETALLTRANSFORM = _descriptor.Descriptor(
  name='GetAllTransform',
  full_name='voldemort.GetAllRequest.GetAllTransform',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='voldemort.GetAllRequest.GetAllTransform.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='transform', full_name='voldemort.GetAllRequest.GetAllTransform.transform', index=1,
      number=2, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=742,
  serialized_end=791,
)

_GETALLREQUEST = _descriptor.Descriptor(
  name='GetAllRequest',
  full_name='voldemort.GetAllRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='keys', full_name='voldemort.GetAllRequest.keys', index=0,
      number=1, type=12, cpp_type=9, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='transforms', full_name='voldemort.GetAllRequest.transforms', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.GetAllRequest.rid', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[_GETALLREQUEST_GETALLTRANSFORM, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=636,
  serialized_end=791,
)


_GETALLRESPONSE = _descriptor.Descriptor(
  name='GetAllResponse',
  full_name='voldemort.GetAllResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='values', full_name='voldemort.GetAllResponse.values', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error', full_name='voldemort.GetAllResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=793,
  serialized_end=884,
)


_PUTREQUEST = _descriptor.Descriptor(
  name='PutRequest',
  full_name='voldemort.PutRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='voldemort.PutRequest.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='versioned', full_name='voldemort.PutRequest.versioned', index=1,
      number=2, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='transforms', full_name='voldemort.PutRequest.transforms', index=2,
      number=3, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.PutRequest.rid', index=3,
      number=4, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=886,
  serialized_end=985,
)


_PUTRESPONSE = _descriptor.Descriptor(
  name='PutResponse',
  full_name='voldemort.PutResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='error', full_name='voldemort.PutResponse.error', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=987,
  serialized_end=1033,
)


_DELETEREQUEST = _descriptor.Descriptor(
  name='DeleteRequest',
  full_name='voldemort.DeleteRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='voldemort.DeleteRequest.key', index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value="",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='version', full_name='voldemort.DeleteRequest.version', index=1,
      number=2, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.DeleteRequest.rid', index=2,
      number=3, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1035,
  serialized_end=1117,
)


_DELETERESPONSE = _descriptor.Descriptor(
  name='DeleteResponse',
  full_name='voldemort.DeleteResponse',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='success', full_name='voldemort.DeleteResponse.success', index=0,
      number=1, type=8, cpp_type=7, label=2,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='error', full_name='voldemort.DeleteResponse.error', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1119,
  serialized_end=1185,
)


_VOLDEMORTREQUEST = _descriptor.Descriptor(
  name='VoldemortRequest',
  full_name='voldemort.VoldemortRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='voldemort.VoldemortRequest.type', index=0,
      number=1, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='should_route', full_name='voldemort.VoldemortRequest.should_route', index=1,
      number=2, type=8, cpp_type=7, label=2,
      has_default_value=True, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='store', full_name='voldemort.VoldemortRequest.store', index=2,
      number=3, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=unicode("", "utf-8"),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='get', full_name='voldemort.VoldemortRequest.get', index=3,
      number=4, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='getAll', full_name='voldemort.VoldemortRequest.getAll', index=4,
      number=5, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='put', full_name='voldemort.VoldemortRequest.put', index=5,
      number=6, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='delete', full_name='voldemort.VoldemortRequest.delete', index=6,
      number=7, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='requestRouteType', full_name='voldemort.VoldemortRequest.requestRouteType', index=7,
      number=8, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  extension_ranges=[],
  serialized_start=1188,
  serialized_end=1470,
)

_VECTORCLOCK.fields_by_name['entries'].message_type = _CLOCKENTRY
_VERSIONED.fields_by_name['version'].message_type = _VECTORCLOCK
_KEYEDVERSIONS.fields_by_name['versions'].message_type = _VERSIONED
_GETRESPONSE.fields_by_name['versioned'].message_type = _VERSIONED
_GETRESPONSE.fields_by_name['error'].message_type = _ERROR
_GETVERSIONRESPONSE.fields_by_name['versions'].message_type = _VECTORCLOCK
_GETVERSIONRESPONSE.fields_by_name['error'].message_type = _ERROR
_GETALLREQUEST_GETALLTRANSFORM.containing_type = _GETALLREQUEST;
_GETALLREQUEST.fields_by_name['transforms'].message_type = _GETALLREQUEST_GETALLTRANSFORM
_GETALLRESPONSE.fields_by_name['values'].message_type = _KEYEDVERSIONS
_GETALLRESPONSE.fields_by_name['error'].message_type = _ERROR
_PUTREQUEST.fields_by_name['versioned'].message_type = _VERSIONED
_PUTRESPONSE.fields_by_name['error'].message_type = _ERROR
_DELETEREQUEST.fields_by_name['version'].message_type = _VECTORCLOCK
_DELETERESPONSE.fields_by_name['error'].message_type = _ERROR
_VOLDEMORTREQUEST.fields_by_name['type'].enum_type = _REQUESTTYPE
_VOLDEMORTREQUEST.fields_by_name['get'].message_type = _GETREQUEST
_VOLDEMORTREQUEST.fields_by_name['getAll'].message_type = _GETALLREQUEST
_VOLDEMORTREQUEST.fields_by_name['put'].message_type = _PUTREQUEST
_VOLDEMORTREQUEST.fields_by_name['delete'].message_type = _DELETEREQUEST
DESCRIPTOR.message_types_by_name['ClockEntry'] = _CLOCKENTRY
DESCRIPTOR.message_types_by_name['VectorClock'] = _VECTORCLOCK
DESCRIPTOR.message_types_by_name['Versioned'] = _VERSIONED
DESCRIPTOR.message_types_by_name['Error'] = _ERROR
DESCRIPTOR.message_types_by_name['KeyedVersions'] = _KEYEDVERSIONS
DESCRIPTOR.message_types_by_name['GetRequest'] = _GETREQUEST
DESCRIPTOR.message_types_by_name['GetResponse'] = _GETRESPONSE
DESCRIPTOR.message_types_by_name['GetVersionResponse'] = _GETVERSIONRESPONSE
DESCRIPTOR.message_types_by_name['GetAllRequest'] = _GETALLREQUEST
DESCRIPTOR.message_types_by_name['GetAllResponse'] = _GETALLRESPONSE
DESCRIPTOR.message_types_by_name['PutRequest'] = _PUTREQUEST
DESCRIPTOR.message_types_by_name['PutResponse'] = _PUTRESPONSE
DESCRIPTOR.message_types_by_name['DeleteRequest'] = _DELETEREQUEST
DESCRIPTOR.message_types_by_name['DeleteResponse'] = _DELETERESPONSE
DESCRIPTOR.message_types_by_name['VoldemortRequest'] = _VOLDEMORTREQUEST

class ClockEntry(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _CLOCKENTRY

  # @@protoc_insertion_point(class_scope:voldemort.ClockEntry)

class VectorClock(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VECTORCLOCK

  # @@protoc_insertion_point(class_scope:voldemort.VectorClock)

class Versioned(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VERSIONED

  # @@protoc_insertion_point(class_scope:voldemort.Versioned)

class Error(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _ERROR

  # @@protoc_insertion_point(class_scope:voldemort.Error)

class KeyedVersions(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _KEYEDVERSIONS

  # @@protoc_insertion_point(class_scope:voldemort.KeyedVersions)

class GetRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETREQUEST

  # @@protoc_insertion_point(class_scope:voldemort.GetRequest)

class GetResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETRESPONSE

  # @@protoc_insertion_point(class_scope:voldemort.GetResponse)

class GetVersionResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETVERSIONRESPONSE

  # @@protoc_insertion_point(class_scope:voldemort.GetVersionResponse)

class GetAllRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType

  class GetAllTransform(_message.Message):
    __metaclass__ = _reflection.GeneratedProtocolMessageType
    DESCRIPTOR = _GETALLREQUEST_GETALLTRANSFORM

    # @@protoc_insertion_point(class_scope:voldemort.GetAllRequest.GetAllTransform)
  DESCRIPTOR = _GETALLREQUEST

  # @@protoc_insertion_point(class_scope:voldemort.GetAllRequest)

class GetAllResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _GETALLRESPONSE

  # @@protoc_insertion_point(class_scope:voldemort.GetAllResponse)

class PutRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _PUTREQUEST

  # @@protoc_insertion_point(class_scope:voldemort.PutRequest)

class PutResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _PUTRESPONSE

  # @@protoc_insertion_point(class_scope:voldemort.PutResponse)

class DeleteRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _DELETEREQUEST

  # @@protoc_insertion_point(class_scope:voldemort.DeleteRequest)

class DeleteResponse(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _DELETERESPONSE

  # @@protoc_insertion_point(class_scope:voldemort.DeleteResponse)

class VoldemortRequest(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _VOLDEMORTREQUEST

  # @@protoc_insertion_point(class_scope:voldemort.VoldemortRequest)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), '\n\034voldemort.client.protocol.pbB\006VProtoH\001')
# @@protoc_insertion_point(module_scope)