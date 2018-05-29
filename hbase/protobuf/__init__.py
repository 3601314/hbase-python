#!/usr/bin/env python3

"""
@author: xi
@since: 2018-05-13
"""

from .AccessControl_pb2 import Permission
from .AccessControl_pb2 import TablePermission
from .AccessControl_pb2 import NamespacePermission
from .AccessControl_pb2 import GlobalPermission
from .AccessControl_pb2 import UserPermission
from .AccessControl_pb2 import UsersAndPermissions
from .AccessControl_pb2 import GrantRequest
from .AccessControl_pb2 import GrantResponse
from .AccessControl_pb2 import RevokeRequest
from .AccessControl_pb2 import RevokeResponse
from .AccessControl_pb2 import GetUserPermissionsRequest
from .AccessControl_pb2 import GetUserPermissionsResponse
from .AccessControl_pb2 import CheckPermissionsRequest
from .AccessControl_pb2 import CheckPermissionsResponse

from .Admin_pb2 import GetRegionInfoRequest
from .Admin_pb2 import GetRegionInfoResponse
from .Admin_pb2 import GetStoreFileRequest
from .Admin_pb2 import GetStoreFileResponse
from .Admin_pb2 import GetOnlineRegionRequest
from .Admin_pb2 import GetOnlineRegionResponse
from .Admin_pb2 import OpenRegionRequest
from .Admin_pb2 import OpenRegionResponse
from .Admin_pb2 import WarmupRegionRequest
from .Admin_pb2 import WarmupRegionResponse
from .Admin_pb2 import CloseRegionRequest
from .Admin_pb2 import CloseRegionResponse
from .Admin_pb2 import FlushRegionRequest
from .Admin_pb2 import FlushRegionResponse
from .Admin_pb2 import SplitRegionRequest
from .Admin_pb2 import SplitRegionResponse
from .Admin_pb2 import CompactRegionRequest
from .Admin_pb2 import CompactRegionResponse
from .Admin_pb2 import UpdateFavoredNodesRequest
from .Admin_pb2 import UpdateFavoredNodesResponse
from .Admin_pb2 import MergeRegionsRequest
from .Admin_pb2 import MergeRegionsResponse
from .Admin_pb2 import WALEntry
from .Admin_pb2 import ReplicateWALEntryRequest
from .Admin_pb2 import ReplicateWALEntryResponse
from .Admin_pb2 import RollWALWriterRequest
from .Admin_pb2 import RollWALWriterResponse
from .Admin_pb2 import StopServerRequest
from .Admin_pb2 import StopServerResponse
from .Admin_pb2 import GetServerInfoRequest
from .Admin_pb2 import ServerInfo
from .Admin_pb2 import GetServerInfoResponse
from .Admin_pb2 import UpdateConfigurationRequest
from .Admin_pb2 import UpdateConfigurationResponse

from .Aggregate_pb2 import AggregateRequest
from .Aggregate_pb2 import AggregateResponse

from .Authentication_pb2 import AuthenticationKey
from .Authentication_pb2 import TokenIdentifier
from .Authentication_pb2 import Token
from .Authentication_pb2 import GetAuthenticationTokenRequest
from .Authentication_pb2 import GetAuthenticationTokenResponse
from .Authentication_pb2 import WhoAmIRequest
from .Authentication_pb2 import WhoAmIResponse

from .Cell_pb2 import Cell
from .Cell_pb2 import KeyValue

from .Client_pb2 import Authorizations
from .Client_pb2 import CellVisibility
from .Client_pb2 import Column
from .Client_pb2 import Get
from .Client_pb2 import Result
from .Client_pb2 import GetRequest
from .Client_pb2 import GetResponse
from .Client_pb2 import Condition
from .Client_pb2 import MutationProto
from .Client_pb2 import MutateRequest
from .Client_pb2 import MutateResponse
from .Client_pb2 import Scan
from .Client_pb2 import ScanRequest
from .Client_pb2 import ScanResponse
from .Client_pb2 import BulkLoadHFileRequest
from .Client_pb2 import BulkLoadHFileResponse
from .Client_pb2 import CoprocessorServiceCall
from .Client_pb2 import CoprocessorServiceResult
from .Client_pb2 import CoprocessorServiceRequest
from .Client_pb2 import CoprocessorServiceResponse
from .Client_pb2 import Action
from .Client_pb2 import RegionAction
from .Client_pb2 import RegionLoadStats
from .Client_pb2 import ResultOrException
from .Client_pb2 import RegionActionResult
from .Client_pb2 import MultiRequest
from .Client_pb2 import MultiResponse

from .ClusterId_pb2 import ClusterId

from .ClusterStatus_pb2 import RegionState
from .ClusterStatus_pb2 import RegionInTransition
from .ClusterStatus_pb2 import StoreSequenceId
from .ClusterStatus_pb2 import RegionStoreSequenceIds
from .ClusterStatus_pb2 import RegionLoad
from .ClusterStatus_pb2 import ReplicationLoadSink
from .ClusterStatus_pb2 import ReplicationLoadSource
from .ClusterStatus_pb2 import ServerLoad
from .ClusterStatus_pb2 import LiveServerInfo
from .ClusterStatus_pb2 import ClusterStatus

from .Comparator_pb2 import Comparator
from .Comparator_pb2 import ByteArrayComparable
from .Comparator_pb2 import BinaryComparator
from .Comparator_pb2 import LongComparator
from .Comparator_pb2 import BinaryPrefixComparator
from .Comparator_pb2 import BitComparator
from .Comparator_pb2 import NullComparator
from .Comparator_pb2 import RegexStringComparator
from .Comparator_pb2 import SubstringComparator

from .Encryption_pb2 import WrappedKey

from .ErrorHandling_pb2 import StackTraceElementMessage
from .ErrorHandling_pb2 import GenericExceptionMessage
from .ErrorHandling_pb2 import ForeignExceptionMessage

from .Filter_pb2 import Filter
from .Filter_pb2 import ColumnCountGetFilter
from .Filter_pb2 import ColumnPaginationFilter
from .Filter_pb2 import ColumnPrefixFilter
from .Filter_pb2 import ColumnRangeFilter
from .Filter_pb2 import CompareFilter
from .Filter_pb2 import DependentColumnFilter
from .Filter_pb2 import FamilyFilter
from .Filter_pb2 import FilterList
from .Filter_pb2 import FilterWrapper
from .Filter_pb2 import FirstKeyOnlyFilter
from .Filter_pb2 import FirstKeyValueMatchingQualifiersFilter
from .Filter_pb2 import FuzzyRowFilter
from .Filter_pb2 import InclusiveStopFilter
from .Filter_pb2 import KeyOnlyFilter
from .Filter_pb2 import MultipleColumnPrefixFilter
from .Filter_pb2 import PageFilter
from .Filter_pb2 import PrefixFilter
from .Filter_pb2 import QualifierFilter
from .Filter_pb2 import RandomRowFilter
from .Filter_pb2 import RowFilter
from .Filter_pb2 import SingleColumnValueExcludeFilter
from .Filter_pb2 import SingleColumnValueFilter
from .Filter_pb2 import SkipFilter
from .Filter_pb2 import TimestampsFilter
from .Filter_pb2 import ValueFilter
from .Filter_pb2 import WhileMatchFilter
from .Filter_pb2 import FilterAllFilter
from .Filter_pb2 import RowRange
from .Filter_pb2 import MultiRowRangeFilter

from .FS_pb2 import HBaseVersionFileContent
from .FS_pb2 import Reference

from .HBase_pb2 import TableName
from .HBase_pb2 import TableSchema
from .HBase_pb2 import ColumnFamilySchema
from .HBase_pb2 import RegionInfo
from .HBase_pb2 import FavoredNodes
from .HBase_pb2 import RegionSpecifier
from .HBase_pb2 import TimeRange
from .HBase_pb2 import ColumnFamilyTimeRange
from .HBase_pb2 import ServerName
from .HBase_pb2 import Coprocessor
from .HBase_pb2 import NameStringPair
from .HBase_pb2 import NameBytesPair
from .HBase_pb2 import BytesBytesPair
from .HBase_pb2 import NameInt64Pair
from .HBase_pb2 import SnapshotDescription
from .HBase_pb2 import ProcedureDescription
from .HBase_pb2 import EmptyMsg
from .HBase_pb2 import LongMsg
from .HBase_pb2 import DoubleMsg
from .HBase_pb2 import BigDecimalMsg
from .HBase_pb2 import UUID
from .HBase_pb2 import NamespaceDescriptor
from .HBase_pb2 import VersionInfo
from .HBase_pb2 import RegionServerInfo

from .HFile_pb2 import FileInfoProto
from .HFile_pb2 import FileTrailerProto

from .LoadBalancer_pb2 import LoadBalancerState

from .MapReduce_pb2 import ScanMetrics
from .MapReduce_pb2 import TableSnapshotRegionSplit

from .MasterProcedure_pb2 import CreateTableStateData
from .MasterProcedure_pb2 import ModifyTableStateData
from .MasterProcedure_pb2 import TruncateTableStateData
from .MasterProcedure_pb2 import DeleteTableStateData
from .MasterProcedure_pb2 import AddColumnFamilyStateData
from .MasterProcedure_pb2 import ModifyColumnFamilyStateData
from .MasterProcedure_pb2 import DeleteColumnFamilyStateData
from .MasterProcedure_pb2 import EnableTableStateData
from .MasterProcedure_pb2 import DisableTableStateData
from .MasterProcedure_pb2 import ServerCrashStateData

from .Master_pb2 import AddColumnRequest
from .Master_pb2 import AddColumnResponse
from .Master_pb2 import DeleteColumnRequest
from .Master_pb2 import DeleteColumnResponse
from .Master_pb2 import ModifyColumnRequest
from .Master_pb2 import ModifyColumnResponse
from .Master_pb2 import MoveRegionRequest
from .Master_pb2 import MoveRegionResponse
from .Master_pb2 import DispatchMergingRegionsRequest
from .Master_pb2 import DispatchMergingRegionsResponse
from .Master_pb2 import AssignRegionRequest
from .Master_pb2 import AssignRegionResponse
from .Master_pb2 import UnassignRegionRequest
from .Master_pb2 import UnassignRegionResponse
from .Master_pb2 import OfflineRegionRequest
from .Master_pb2 import OfflineRegionResponse
from .Master_pb2 import CreateTableRequest
from .Master_pb2 import CreateTableResponse
from .Master_pb2 import DeleteTableRequest
from .Master_pb2 import DeleteTableResponse
from .Master_pb2 import TruncateTableRequest
from .Master_pb2 import TruncateTableResponse
from .Master_pb2 import EnableTableRequest
from .Master_pb2 import EnableTableResponse
from .Master_pb2 import DisableTableRequest
from .Master_pb2 import DisableTableResponse
from .Master_pb2 import ModifyTableRequest
from .Master_pb2 import ModifyTableResponse
from .Master_pb2 import CreateNamespaceRequest
from .Master_pb2 import CreateNamespaceResponse
from .Master_pb2 import DeleteNamespaceRequest
from .Master_pb2 import DeleteNamespaceResponse
from .Master_pb2 import ModifyNamespaceRequest
from .Master_pb2 import ModifyNamespaceResponse
from .Master_pb2 import GetNamespaceDescriptorRequest
from .Master_pb2 import GetNamespaceDescriptorResponse
from .Master_pb2 import ListNamespaceDescriptorsRequest
from .Master_pb2 import ListNamespaceDescriptorsResponse
from .Master_pb2 import ListTableDescriptorsByNamespaceRequest
from .Master_pb2 import ListTableDescriptorsByNamespaceResponse
from .Master_pb2 import ListTableNamesByNamespaceRequest
from .Master_pb2 import ListTableNamesByNamespaceResponse
from .Master_pb2 import ShutdownRequest
from .Master_pb2 import ShutdownResponse
from .Master_pb2 import StopMasterRequest
from .Master_pb2 import StopMasterResponse
from .Master_pb2 import BalanceRequest
from .Master_pb2 import BalanceResponse
from .Master_pb2 import SetBalancerRunningRequest
from .Master_pb2 import SetBalancerRunningResponse
from .Master_pb2 import IsBalancerEnabledRequest
from .Master_pb2 import IsBalancerEnabledResponse
from .Master_pb2 import NormalizeRequest
from .Master_pb2 import NormalizeResponse
from .Master_pb2 import SetNormalizerRunningRequest
from .Master_pb2 import SetNormalizerRunningResponse
from .Master_pb2 import IsNormalizerEnabledRequest
from .Master_pb2 import IsNormalizerEnabledResponse
from .Master_pb2 import RunCatalogScanRequest
from .Master_pb2 import RunCatalogScanResponse
from .Master_pb2 import EnableCatalogJanitorRequest
from .Master_pb2 import EnableCatalogJanitorResponse
from .Master_pb2 import IsCatalogJanitorEnabledRequest
from .Master_pb2 import IsCatalogJanitorEnabledResponse
from .Master_pb2 import SnapshotRequest
from .Master_pb2 import SnapshotResponse
from .Master_pb2 import GetCompletedSnapshotsRequest
from .Master_pb2 import GetCompletedSnapshotsResponse
from .Master_pb2 import DeleteSnapshotRequest
from .Master_pb2 import DeleteSnapshotResponse
from .Master_pb2 import RestoreSnapshotRequest
from .Master_pb2 import RestoreSnapshotResponse
from .Master_pb2 import IsSnapshotDoneRequest
from .Master_pb2 import IsSnapshotDoneResponse
from .Master_pb2 import IsRestoreSnapshotDoneRequest
from .Master_pb2 import IsRestoreSnapshotDoneResponse
from .Master_pb2 import GetSchemaAlterStatusRequest
from .Master_pb2 import GetSchemaAlterStatusResponse
from .Master_pb2 import GetTableDescriptorsRequest
from .Master_pb2 import GetTableDescriptorsResponse
from .Master_pb2 import GetTableNamesRequest
from .Master_pb2 import GetTableNamesResponse
from .Master_pb2 import GetClusterStatusRequest
from .Master_pb2 import GetClusterStatusResponse
from .Master_pb2 import IsMasterRunningRequest
from .Master_pb2 import IsMasterRunningResponse
from .Master_pb2 import ExecProcedureRequest
from .Master_pb2 import ExecProcedureResponse
from .Master_pb2 import IsProcedureDoneRequest
from .Master_pb2 import IsProcedureDoneResponse
from .Master_pb2 import GetProcedureResultRequest
from .Master_pb2 import GetProcedureResultResponse
from .Master_pb2 import AbortProcedureRequest
from .Master_pb2 import AbortProcedureResponse
from .Master_pb2 import ListProceduresRequest
from .Master_pb2 import ListProceduresResponse
from .Master_pb2 import SetQuotaRequest
from .Master_pb2 import SetQuotaResponse
from .Master_pb2 import MajorCompactionTimestampRequest
from .Master_pb2 import MajorCompactionTimestampForRegionRequest
from .Master_pb2 import MajorCompactionTimestampResponse
from .Master_pb2 import SecurityCapabilitiesRequest
from .Master_pb2 import SecurityCapabilitiesResponse

from .MultiRowMutation_pb2 import MutateRowsRequest
from .MultiRowMutation_pb2 import MutateRowsResponse

from .Procedure_pb2 import Procedure
from .Procedure_pb2 import SequentialProcedureData
from .Procedure_pb2 import StateMachineProcedureData
from .Procedure_pb2 import ProcedureWALHeader
from .Procedure_pb2 import ProcedureWALTrailer
from .Procedure_pb2 import ProcedureStoreTracker
from .Procedure_pb2 import ProcedureWALEntry

from .Quota_pb2 import TimedQuota
from .Quota_pb2 import Throttle
from .Quota_pb2 import ThrottleRequest
from .Quota_pb2 import Quotas
from .Quota_pb2 import QuotaUsage

from .RegionNormalizer_pb2 import RegionNormalizerState

from .RegionServerStatus_pb2 import RegionServerStartupRequest
from .RegionServerStatus_pb2 import RegionServerStartupResponse
from .RegionServerStatus_pb2 import RegionServerReportRequest
from .RegionServerStatus_pb2 import RegionServerReportResponse
from .RegionServerStatus_pb2 import ReportRSFatalErrorRequest
from .RegionServerStatus_pb2 import ReportRSFatalErrorResponse
from .RegionServerStatus_pb2 import GetLastFlushedSequenceIdRequest
from .RegionServerStatus_pb2 import GetLastFlushedSequenceIdResponse
from .RegionServerStatus_pb2 import RegionStateTransition
from .RegionServerStatus_pb2 import ReportRegionStateTransitionRequest
from .RegionServerStatus_pb2 import ReportRegionStateTransitionResponse

from .RowProcessor_pb2 import ProcessRequest
from .RowProcessor_pb2 import ProcessResponse

from .RPC_pb2 import UserInformation
from .RPC_pb2 import ConnectionHeader
from .RPC_pb2 import CellBlockMeta
from .RPC_pb2 import ExceptionResponse
from .RPC_pb2 import RequestHeader
from .RPC_pb2 import ResponseHeader

from .SecureBulkLoad_pb2 import SecureBulkLoadHFilesRequest
from .SecureBulkLoad_pb2 import SecureBulkLoadHFilesResponse
from .SecureBulkLoad_pb2 import DelegationToken
from .SecureBulkLoad_pb2 import PrepareBulkLoadRequest
from .SecureBulkLoad_pb2 import PrepareBulkLoadResponse
from .SecureBulkLoad_pb2 import CleanupBulkLoadRequest
from .SecureBulkLoad_pb2 import CleanupBulkLoadResponse

from .Snapshot_pb2 import SnapshotFileInfo
from .Snapshot_pb2 import SnapshotRegionManifest
from .Snapshot_pb2 import SnapshotDataManifest

from .Tracing_pb2 import RPCTInfo

from .VisibilityLabels_pb2 import VisibilityLabelsRequest
from .VisibilityLabels_pb2 import VisibilityLabel
from .VisibilityLabels_pb2 import VisibilityLabelsResponse
from .VisibilityLabels_pb2 import SetAuthsRequest
from .VisibilityLabels_pb2 import UserAuthorizations
from .VisibilityLabels_pb2 import MultiUserAuthorizations
from .VisibilityLabels_pb2 import GetAuthsRequest
from .VisibilityLabels_pb2 import GetAuthsResponse
from .VisibilityLabels_pb2 import ListLabelsRequest
from .VisibilityLabels_pb2 import ListLabelsResponse

from .WAL_pb2 import WALHeader
from .WAL_pb2 import WALKey
from .WAL_pb2 import FamilyScope
from .WAL_pb2 import CompactionDescriptor
from .WAL_pb2 import FlushDescriptor
from .WAL_pb2 import StoreDescriptor
from .WAL_pb2 import BulkLoadDescriptor
from .WAL_pb2 import RegionEventDescriptor
from .WAL_pb2 import WALTrailer

from .ZooKeeper_pb2 import MetaRegionServer
from .ZooKeeper_pb2 import Master
from .ZooKeeper_pb2 import ClusterUp
from .ZooKeeper_pb2 import RegionTransition
from .ZooKeeper_pb2 import SplitLogTask
from .ZooKeeper_pb2 import Table
from .ZooKeeper_pb2 import ReplicationPeer
from .ZooKeeper_pb2 import ReplicationState
from .ZooKeeper_pb2 import ReplicationHLogPosition
from .ZooKeeper_pb2 import ReplicationLock
from .ZooKeeper_pb2 import TableLock

REQUEST_TYPES = {
    GrantRequest: 'Grant',
    RevokeRequest: 'Revoke',
    GetUserPermissionsRequest: 'GetUserPermissions',
    CheckPermissionsRequest: 'CheckPermissions',
    GetRegionInfoRequest: 'GetRegionInfo',
    GetStoreFileRequest: 'GetStoreFile',
    GetOnlineRegionRequest: 'GetOnlineRegion',
    OpenRegionRequest: 'OpenRegion',
    WarmupRegionRequest: 'WarmupRegion',
    CloseRegionRequest: 'CloseRegion',
    FlushRegionRequest: 'FlushRegion',
    SplitRegionRequest: 'SplitRegion',
    CompactRegionRequest: 'CompactRegion',
    UpdateFavoredNodesRequest: 'UpdateFavoredNodes',
    MergeRegionsRequest: 'MergeRegions',
    ReplicateWALEntryRequest: 'ReplicateWALEntry',
    RollWALWriterRequest: 'RollWALWriter',
    StopServerRequest: 'StopServer',
    GetServerInfoRequest: 'GetServerInfo',
    UpdateConfigurationRequest: 'UpdateConfiguration',
    AggregateRequest: 'Aggregate',
    GetAuthenticationTokenRequest: 'GetAuthenticationToken',
    WhoAmIRequest: 'WhoAmI',
    GetRequest: 'Get',
    MutateRequest: 'Mutate',
    ScanRequest: 'Scan',
    BulkLoadHFileRequest: 'BulkLoadHFile',
    CoprocessorServiceRequest: 'CoprocessorService',
    MultiRequest: 'Multi',
    AddColumnRequest: 'AddColumn',
    DeleteColumnRequest: 'DeleteColumn',
    ModifyColumnRequest: 'ModifyColumn',
    MoveRegionRequest: 'MoveRegion',
    DispatchMergingRegionsRequest: 'DispatchMergingRegions',
    AssignRegionRequest: 'AssignRegion',
    UnassignRegionRequest: 'UnassignRegion',
    OfflineRegionRequest: 'OfflineRegion',
    CreateTableRequest: 'CreateTable',
    DeleteTableRequest: 'DeleteTable',
    TruncateTableRequest: 'TruncateTable',
    EnableTableRequest: 'EnableTable',
    DisableTableRequest: 'DisableTable',
    ModifyTableRequest: 'ModifyTable',
    CreateNamespaceRequest: 'CreateNamespace',
    DeleteNamespaceRequest: 'DeleteNamespace',
    ModifyNamespaceRequest: 'ModifyNamespace',
    GetNamespaceDescriptorRequest: 'GetNamespaceDescriptor',
    ListNamespaceDescriptorsRequest: 'ListNamespaceDescriptors',
    ListTableDescriptorsByNamespaceRequest: 'ListTableDescriptorsByNamespace',
    ListTableNamesByNamespaceRequest: 'ListTableNamesByNamespace',
    ShutdownRequest: 'Shutdown',
    StopMasterRequest: 'StopMaster',
    BalanceRequest: 'Balance',
    SetBalancerRunningRequest: 'SetBalancerRunning',
    IsBalancerEnabledRequest: 'IsBalancerEnabled',
    NormalizeRequest: 'Normalize',
    SetNormalizerRunningRequest: 'SetNormalizerRunning',
    IsNormalizerEnabledRequest: 'IsNormalizerEnabled',
    RunCatalogScanRequest: 'RunCatalogScan',
    EnableCatalogJanitorRequest: 'EnableCatalogJanitor',
    IsCatalogJanitorEnabledRequest: 'IsCatalogJanitorEnabled',
    SnapshotRequest: 'Snapshot',
    GetCompletedSnapshotsRequest: 'GetCompletedSnapshots',
    DeleteSnapshotRequest: 'DeleteSnapshot',
    RestoreSnapshotRequest: 'RestoreSnapshot',
    IsSnapshotDoneRequest: 'IsSnapshotDone',
    IsRestoreSnapshotDoneRequest: 'IsRestoreSnapshotDone',
    GetSchemaAlterStatusRequest: 'GetSchemaAlterStatus',
    GetTableDescriptorsRequest: 'GetTableDescriptors',
    GetTableNamesRequest: 'GetTableNames',
    GetClusterStatusRequest: 'GetClusterStatus',
    IsMasterRunningRequest: 'IsMasterRunning',
    ExecProcedureRequest: 'ExecProcedure',
    IsProcedureDoneRequest: 'IsProcedureDone',
    GetProcedureResultRequest: 'getProcedureResult',
    AbortProcedureRequest: 'AbortProcedure',
    ListProceduresRequest: 'ListProcedures',
    SetQuotaRequest: 'SetQuota',
    MajorCompactionTimestampRequest: 'MajorCompactionTimestamp',
    MajorCompactionTimestampForRegionRequest: 'MajorCompactionTimestampForRegion',
    SecurityCapabilitiesRequest: 'SecurityCapabilities',
    MutateRowsRequest: 'MutateRows',
    ThrottleRequest: 'Throttle',
    RegionServerStartupRequest: 'RegionServerStartup',
    RegionServerReportRequest: 'RegionServerReport',
    ReportRSFatalErrorRequest: 'ReportRSFatalError',
    GetLastFlushedSequenceIdRequest: 'GetLastFlushedSequenceId',
    ReportRegionStateTransitionRequest: 'ReportRegionStateTransition',
    ProcessRequest: 'Process',
    SecureBulkLoadHFilesRequest: 'SecureBulkLoadHFiles',
    PrepareBulkLoadRequest: 'PrepareBulkLoad',
    CleanupBulkLoadRequest: 'CleanupBulkLoad',
    VisibilityLabelsRequest: 'VisibilityLabels',
    SetAuthsRequest: 'SetAuths',
    GetAuthsRequest: 'GetAuths',
    ListLabelsRequest: 'ListLabels'
}

RESPONSE_TYPES = {
    'Grant': GrantResponse,
    'Revoke': RevokeResponse,
    'GetUserPermissions': GetUserPermissionsResponse,
    'CheckPermissions': CheckPermissionsResponse,
    'GetRegionInfo': GetRegionInfoResponse,
    'GetStoreFile': GetStoreFileResponse,
    'GetOnlineRegion': GetOnlineRegionResponse,
    'OpenRegion': OpenRegionResponse,
    'WarmupRegion': WarmupRegionResponse,
    'CloseRegion': CloseRegionResponse,
    'FlushRegion': FlushRegionResponse,
    'SplitRegion': SplitRegionResponse,
    'CompactRegion': CompactRegionResponse,
    'UpdateFavoredNodes': UpdateFavoredNodesResponse,
    'MergeRegions': MergeRegionsResponse,
    'ReplicateWALEntry': ReplicateWALEntryResponse,
    'RollWALWriter': RollWALWriterResponse,
    'StopServer': StopServerResponse,
    'GetServerInfo': GetServerInfoResponse,
    'UpdateConfiguration': UpdateConfigurationResponse,
    'Aggregate': AggregateResponse,
    'GetAuthenticationToken': GetAuthenticationTokenResponse,
    'WhoAmI': WhoAmIResponse,
    'Get': GetResponse,
    'Mutate': MutateResponse,
    'Scan': ScanResponse,
    'BulkLoadHFile': BulkLoadHFileResponse,
    'CoprocessorService': CoprocessorServiceResponse,
    'Multi': MultiResponse,
    'AddColumn': AddColumnResponse,
    'DeleteColumn': DeleteColumnResponse,
    'ModifyColumn': ModifyColumnResponse,
    'MoveRegion': MoveRegionResponse,
    'DispatchMergingRegions': DispatchMergingRegionsResponse,
    'AssignRegion': AssignRegionResponse,
    'UnassignRegion': UnassignRegionResponse,
    'OfflineRegion': OfflineRegionResponse,
    'CreateTable': CreateTableResponse,
    'DeleteTable': DeleteTableResponse,
    'TruncateTable': TruncateTableResponse,
    'EnableTable': EnableTableResponse,
    'DisableTable': DisableTableResponse,
    'ModifyTable': ModifyTableResponse,
    'CreateNamespace': CreateNamespaceResponse,
    'DeleteNamespace': DeleteNamespaceResponse,
    'ModifyNamespace': ModifyNamespaceResponse,
    'GetNamespaceDescriptor': GetNamespaceDescriptorResponse,
    'ListNamespaceDescriptors': ListNamespaceDescriptorsResponse,
    'ListTableDescriptorsByNamespace': ListTableDescriptorsByNamespaceResponse,
    'ListTableNamesByNamespace': ListTableNamesByNamespaceResponse,
    'Shutdown': ShutdownResponse,
    'StopMaster': StopMasterResponse,
    'Balance': BalanceResponse,
    'SetBalancerRunning': SetBalancerRunningResponse,
    'IsBalancerEnabled': IsBalancerEnabledResponse,
    'Normalize': NormalizeResponse,
    'SetNormalizerRunning': SetNormalizerRunningResponse,
    'IsNormalizerEnabled': IsNormalizerEnabledResponse,
    'RunCatalogScan': RunCatalogScanResponse,
    'EnableCatalogJanitor': EnableCatalogJanitorResponse,
    'IsCatalogJanitorEnabled': IsCatalogJanitorEnabledResponse,
    'Snapshot': SnapshotResponse,
    'GetCompletedSnapshots': GetCompletedSnapshotsResponse,
    'DeleteSnapshot': DeleteSnapshotResponse,
    'RestoreSnapshot': RestoreSnapshotResponse,
    'IsSnapshotDone': IsSnapshotDoneResponse,
    'IsRestoreSnapshotDone': IsRestoreSnapshotDoneResponse,
    'GetSchemaAlterStatus': GetSchemaAlterStatusResponse,
    'GetTableDescriptors': GetTableDescriptorsResponse,
    'GetTableNames': GetTableNamesResponse,
    'GetClusterStatus': GetClusterStatusResponse,
    'IsMasterRunning': IsMasterRunningResponse,
    'ExecProcedure': ExecProcedureResponse,
    'IsProcedureDone': IsProcedureDoneResponse,
    'getProcedureResult': GetProcedureResultResponse,
    'AbortProcedure': AbortProcedureResponse,
    'ListProcedures': ListProceduresResponse,
    'SetQuota': SetQuotaResponse,
    'MajorCompactionTimestamp': MajorCompactionTimestampResponse,
    'SecurityCapabilities': SecurityCapabilitiesResponse,
    'MutateRows': MutateRowsResponse,
    'RegionServerStartup': RegionServerStartupResponse,
    'RegionServerReport': RegionServerReportResponse,
    'ReportRSFatalError': ReportRSFatalErrorResponse,
    'GetLastFlushedSequenceId': GetLastFlushedSequenceIdResponse,
    'ReportRegionStateTransition': ReportRegionStateTransitionResponse,
    'Process': ProcessResponse,
    'Exception': ExceptionResponse,
    'SecureBulkLoadHFiles': SecureBulkLoadHFilesResponse,
    'PrepareBulkLoad': PrepareBulkLoadResponse,
    'CleanupBulkLoad': CleanupBulkLoadResponse,
    'VisibilityLabels': VisibilityLabelsResponse,
    'GetAuths': GetAuthsResponse,
    'ListLabels': ListLabelsResponse
}


def get_request_name(req_obj):
    return REQUEST_TYPES[type(req_obj)]


def get_response_object(type_name):
    return RESPONSE_TYPES[type_name]()
