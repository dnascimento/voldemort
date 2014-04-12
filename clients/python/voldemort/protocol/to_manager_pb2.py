# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: to-manager.proto

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)




DESCRIPTOR = _descriptor.FileDescriptor(
  name='to-manager.proto',
  package='voldemort',
  serialized_pb='\n\x10to-manager.proto\x12\tvoldemort\"\x93\x01\n\x0cMsgToManager\x12%\n\x08trackMsg\x18\x01 \x01(\x0b\x32\x13.voldemort.TrackMsg\x12+\n\x0bstartEndMsg\x18\x02 \x01(\x0b\x32\x16.voldemort.StartEndMsg\x12/\n\x12trackMsgFromClient\x18\x03 \x01(\x0b\x32\x13.voldemort.TrackMsg\"@\n\x08TrackMsg\x12$\n\x05\x65ntry\x18\x01 \x03(\x0b\x32\x15.voldemort.TrackEntry\x12\x0e\n\x06nodeId\x18\x02 \x01(\t\"-\n\nTrackEntry\x12\x0b\n\x03rid\x18\x01 \x02(\x03\x12\x12\n\ndependency\x18\x02 \x03(\x03\"4\n\x0bStartEndMsg\x12%\n\x03msg\x18\x01 \x03(\x0b\x32\x18.voldemort.StartEndEntry\"+\n\rStartEndEntry\x12\r\n\x05start\x18\x01 \x02(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x02(\x03\"4\n\x03RUD\x12\x0b\n\x03rid\x18\x01 \x01(\x03\x12\x0e\n\x06\x62ranch\x18\x02 \x01(\x05\x12\x10\n\x08restrain\x18\x03 \x01(\x08\x42/\n\x1bvoldemort.undoTracker.protoB\x0eToManagerProtoH\x01')




_MSGTOMANAGER = _descriptor.Descriptor(
  name='MsgToManager',
  full_name='voldemort.MsgToManager',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='trackMsg', full_name='voldemort.MsgToManager.trackMsg', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='startEndMsg', full_name='voldemort.MsgToManager.startEndMsg', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='trackMsgFromClient', full_name='voldemort.MsgToManager.trackMsgFromClient', index=2,
      number=3, type=11, cpp_type=10, label=1,
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
  serialized_start=32,
  serialized_end=179,
)


_TRACKMSG = _descriptor.Descriptor(
  name='TrackMsg',
  full_name='voldemort.TrackMsg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='entry', full_name='voldemort.TrackMsg.entry', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='nodeId', full_name='voldemort.TrackMsg.nodeId', index=1,
      number=2, type=9, cpp_type=9, label=1,
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
  serialized_start=181,
  serialized_end=245,
)


_TRACKENTRY = _descriptor.Descriptor(
  name='TrackEntry',
  full_name='voldemort.TrackEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.TrackEntry.rid', index=0,
      number=1, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='dependency', full_name='voldemort.TrackEntry.dependency', index=1,
      number=2, type=3, cpp_type=2, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=247,
  serialized_end=292,
)


_STARTENDMSG = _descriptor.Descriptor(
  name='StartEndMsg',
  full_name='voldemort.StartEndMsg',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='msg', full_name='voldemort.StartEndMsg.msg', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
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
  serialized_start=294,
  serialized_end=346,
)


_STARTENDENTRY = _descriptor.Descriptor(
  name='StartEndEntry',
  full_name='voldemort.StartEndEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='start', full_name='voldemort.StartEndEntry.start', index=0,
      number=1, type=3, cpp_type=2, label=2,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='end', full_name='voldemort.StartEndEntry.end', index=1,
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
  serialized_start=348,
  serialized_end=391,
)


_RUD = _descriptor.Descriptor(
  name='RUD',
  full_name='voldemort.RUD',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='rid', full_name='voldemort.RUD.rid', index=0,
      number=1, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='branch', full_name='voldemort.RUD.branch', index=1,
      number=2, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None),
    _descriptor.FieldDescriptor(
      name='restrain', full_name='voldemort.RUD.restrain', index=2,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
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
  serialized_start=393,
  serialized_end=445,
)

_MSGTOMANAGER.fields_by_name['trackMsg'].message_type = _TRACKMSG
_MSGTOMANAGER.fields_by_name['startEndMsg'].message_type = _STARTENDMSG
_MSGTOMANAGER.fields_by_name['trackMsgFromClient'].message_type = _TRACKMSG
_TRACKMSG.fields_by_name['entry'].message_type = _TRACKENTRY
_STARTENDMSG.fields_by_name['msg'].message_type = _STARTENDENTRY
DESCRIPTOR.message_types_by_name['MsgToManager'] = _MSGTOMANAGER
DESCRIPTOR.message_types_by_name['TrackMsg'] = _TRACKMSG
DESCRIPTOR.message_types_by_name['TrackEntry'] = _TRACKENTRY
DESCRIPTOR.message_types_by_name['StartEndMsg'] = _STARTENDMSG
DESCRIPTOR.message_types_by_name['StartEndEntry'] = _STARTENDENTRY
DESCRIPTOR.message_types_by_name['RUD'] = _RUD

class MsgToManager(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _MSGTOMANAGER

  # @@protoc_insertion_point(class_scope:voldemort.MsgToManager)

class TrackMsg(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _TRACKMSG

  # @@protoc_insertion_point(class_scope:voldemort.TrackMsg)

class TrackEntry(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _TRACKENTRY

  # @@protoc_insertion_point(class_scope:voldemort.TrackEntry)

class StartEndMsg(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _STARTENDMSG

  # @@protoc_insertion_point(class_scope:voldemort.StartEndMsg)

class StartEndEntry(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _STARTENDENTRY

  # @@protoc_insertion_point(class_scope:voldemort.StartEndEntry)

class RUD(_message.Message):
  __metaclass__ = _reflection.GeneratedProtocolMessageType
  DESCRIPTOR = _RUD

  # @@protoc_insertion_point(class_scope:voldemort.RUD)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), '\n\033voldemort.undoTracker.protoB\016ToManagerProtoH\001')
# @@protoc_insertion_point(module_scope)
