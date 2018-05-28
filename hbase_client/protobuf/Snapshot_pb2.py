# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: Snapshot.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from . import FS_pb2 as FS__pb2
from . import HBase_pb2 as HBase__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='Snapshot.proto',
  package='',
  syntax='proto2',
  serialized_pb=_b('\n\x0eSnapshot.proto\x1a\x08\x46S.proto\x1a\x0bHBase.proto\"\x89\x01\n\x10SnapshotFileInfo\x12$\n\x04type\x18\x01 \x02(\x0e\x32\x16.SnapshotFileInfo.Type\x12\r\n\x05hfile\x18\x03 \x01(\t\x12\x12\n\nwal_server\x18\x04 \x01(\t\x12\x10\n\x08wal_name\x18\x05 \x01(\t\"\x1a\n\x04Type\x12\t\n\x05HFILE\x10\x01\x12\x07\n\x03WAL\x10\x02\"\xaf\x02\n\x16SnapshotRegionManifest\x12\x0f\n\x07version\x18\x01 \x01(\x05\x12 \n\x0bregion_info\x18\x02 \x02(\x0b\x32\x0b.RegionInfo\x12\x39\n\x0c\x66\x61mily_files\x18\x03 \x03(\x0b\x32#.SnapshotRegionManifest.FamilyFiles\x1aK\n\tStoreFile\x12\x0c\n\x04name\x18\x01 \x02(\t\x12\x1d\n\treference\x18\x02 \x01(\x0b\x32\n.Reference\x12\x11\n\tfile_size\x18\x03 \x01(\x04\x1aZ\n\x0b\x46\x61milyFiles\x12\x13\n\x0b\x66\x61mily_name\x18\x01 \x02(\x0c\x12\x36\n\x0bstore_files\x18\x02 \x03(\x0b\x32!.SnapshotRegionManifest.StoreFile\"m\n\x14SnapshotDataManifest\x12\"\n\x0ctable_schema\x18\x01 \x02(\x0b\x32\x0c.TableSchema\x12\x31\n\x10region_manifests\x18\x02 \x03(\x0b\x32\x17.SnapshotRegionManifestBD\n*org.apache.hadoop.hbase.protobuf.generatedB\x0eSnapshotProtosH\x01\x88\x01\x01\xa0\x01\x01')
  ,
  dependencies=[FS__pb2.DESCRIPTOR,HBase__pb2.DESCRIPTOR,])



_SNAPSHOTFILEINFO_TYPE = _descriptor.EnumDescriptor(
  name='Type',
  full_name='SnapshotFileInfo.Type',
  filename=None,
  file=DESCRIPTOR,
  values=[
    _descriptor.EnumValueDescriptor(
      name='HFILE', index=0, number=1,
      options=None,
      type=None),
    _descriptor.EnumValueDescriptor(
      name='WAL', index=1, number=2,
      options=None,
      type=None),
  ],
  containing_type=None,
  options=None,
  serialized_start=153,
  serialized_end=179,
)
_sym_db.RegisterEnumDescriptor(_SNAPSHOTFILEINFO_TYPE)


_SNAPSHOTFILEINFO = _descriptor.Descriptor(
  name='SnapshotFileInfo',
  full_name='SnapshotFileInfo',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='type', full_name='SnapshotFileInfo.type', index=0,
      number=1, type=14, cpp_type=8, label=2,
      has_default_value=False, default_value=1,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='hfile', full_name='SnapshotFileInfo.hfile', index=1,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='wal_server', full_name='SnapshotFileInfo.wal_server', index=2,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='wal_name', full_name='SnapshotFileInfo.wal_name', index=3,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
    _SNAPSHOTFILEINFO_TYPE,
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=42,
  serialized_end=179,
)


_SNAPSHOTREGIONMANIFEST_STOREFILE = _descriptor.Descriptor(
  name='StoreFile',
  full_name='SnapshotRegionManifest.StoreFile',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='name', full_name='SnapshotRegionManifest.StoreFile.name', index=0,
      number=1, type=9, cpp_type=9, label=2,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='reference', full_name='SnapshotRegionManifest.StoreFile.reference', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='file_size', full_name='SnapshotRegionManifest.StoreFile.file_size', index=2,
      number=3, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=318,
  serialized_end=393,
)

_SNAPSHOTREGIONMANIFEST_FAMILYFILES = _descriptor.Descriptor(
  name='FamilyFiles',
  full_name='SnapshotRegionManifest.FamilyFiles',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='family_name', full_name='SnapshotRegionManifest.FamilyFiles.family_name', index=0,
      number=1, type=12, cpp_type=9, label=2,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='store_files', full_name='SnapshotRegionManifest.FamilyFiles.store_files', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=395,
  serialized_end=485,
)

_SNAPSHOTREGIONMANIFEST = _descriptor.Descriptor(
  name='SnapshotRegionManifest',
  full_name='SnapshotRegionManifest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='version', full_name='SnapshotRegionManifest.version', index=0,
      number=1, type=5, cpp_type=1, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='region_info', full_name='SnapshotRegionManifest.region_info', index=1,
      number=2, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='family_files', full_name='SnapshotRegionManifest.family_files', index=2,
      number=3, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[_SNAPSHOTREGIONMANIFEST_STOREFILE, _SNAPSHOTREGIONMANIFEST_FAMILYFILES, ],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=182,
  serialized_end=485,
)


_SNAPSHOTDATAMANIFEST = _descriptor.Descriptor(
  name='SnapshotDataManifest',
  full_name='SnapshotDataManifest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='table_schema', full_name='SnapshotDataManifest.table_schema', index=0,
      number=1, type=11, cpp_type=10, label=2,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='region_manifests', full_name='SnapshotDataManifest.region_manifests', index=1,
      number=2, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto2',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=487,
  serialized_end=596,
)

_SNAPSHOTFILEINFO.fields_by_name['type'].enum_type = _SNAPSHOTFILEINFO_TYPE
_SNAPSHOTFILEINFO_TYPE.containing_type = _SNAPSHOTFILEINFO
_SNAPSHOTREGIONMANIFEST_STOREFILE.fields_by_name['reference'].message_type = FS__pb2._REFERENCE
_SNAPSHOTREGIONMANIFEST_STOREFILE.containing_type = _SNAPSHOTREGIONMANIFEST
_SNAPSHOTREGIONMANIFEST_FAMILYFILES.fields_by_name['store_files'].message_type = _SNAPSHOTREGIONMANIFEST_STOREFILE
_SNAPSHOTREGIONMANIFEST_FAMILYFILES.containing_type = _SNAPSHOTREGIONMANIFEST
_SNAPSHOTREGIONMANIFEST.fields_by_name['region_info'].message_type = HBase__pb2._REGIONINFO
_SNAPSHOTREGIONMANIFEST.fields_by_name['family_files'].message_type = _SNAPSHOTREGIONMANIFEST_FAMILYFILES
_SNAPSHOTDATAMANIFEST.fields_by_name['table_schema'].message_type = HBase__pb2._TABLESCHEMA
_SNAPSHOTDATAMANIFEST.fields_by_name['region_manifests'].message_type = _SNAPSHOTREGIONMANIFEST
DESCRIPTOR.message_types_by_name['SnapshotFileInfo'] = _SNAPSHOTFILEINFO
DESCRIPTOR.message_types_by_name['SnapshotRegionManifest'] = _SNAPSHOTREGIONMANIFEST
DESCRIPTOR.message_types_by_name['SnapshotDataManifest'] = _SNAPSHOTDATAMANIFEST
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

SnapshotFileInfo = _reflection.GeneratedProtocolMessageType('SnapshotFileInfo', (_message.Message,), dict(
  DESCRIPTOR = _SNAPSHOTFILEINFO,
  __module__ = 'Snapshot_pb2'
  # @@protoc_insertion_point(class_scope:SnapshotFileInfo)
  ))
_sym_db.RegisterMessage(SnapshotFileInfo)

SnapshotRegionManifest = _reflection.GeneratedProtocolMessageType('SnapshotRegionManifest', (_message.Message,), dict(

  StoreFile = _reflection.GeneratedProtocolMessageType('StoreFile', (_message.Message,), dict(
    DESCRIPTOR = _SNAPSHOTREGIONMANIFEST_STOREFILE,
    __module__ = 'Snapshot_pb2'
    # @@protoc_insertion_point(class_scope:SnapshotRegionManifest.StoreFile)
    ))
  ,

  FamilyFiles = _reflection.GeneratedProtocolMessageType('FamilyFiles', (_message.Message,), dict(
    DESCRIPTOR = _SNAPSHOTREGIONMANIFEST_FAMILYFILES,
    __module__ = 'Snapshot_pb2'
    # @@protoc_insertion_point(class_scope:SnapshotRegionManifest.FamilyFiles)
    ))
  ,
  DESCRIPTOR = _SNAPSHOTREGIONMANIFEST,
  __module__ = 'Snapshot_pb2'
  # @@protoc_insertion_point(class_scope:SnapshotRegionManifest)
  ))
_sym_db.RegisterMessage(SnapshotRegionManifest)
_sym_db.RegisterMessage(SnapshotRegionManifest.StoreFile)
_sym_db.RegisterMessage(SnapshotRegionManifest.FamilyFiles)

SnapshotDataManifest = _reflection.GeneratedProtocolMessageType('SnapshotDataManifest', (_message.Message,), dict(
  DESCRIPTOR = _SNAPSHOTDATAMANIFEST,
  __module__ = 'Snapshot_pb2'
  # @@protoc_insertion_point(class_scope:SnapshotDataManifest)
  ))
_sym_db.RegisterMessage(SnapshotDataManifest)


DESCRIPTOR.has_options = True
DESCRIPTOR._options = _descriptor._ParseOptions(descriptor_pb2.FileOptions(), _b('\n*org.apache.hadoop.hbase.protobuf.generatedB\016SnapshotProtosH\001\210\001\001\240\001\001'))
# @@protoc_insertion_point(module_scope)