//
//  QSCloudKitHelper.m
//  Quikstudy
//
//  Created by Manuel Entrena on 26/05/2016.
//  Copyright © 2016 Manuel Entrena. All rights reserved.
//

#import "QSCloudKitSynchronizer.h"
#import "SyncKitLog.h"
#import "QSBackupDetection.h"
#import "QSCloudKitSynchronizer+Private.h"
#import <SyncKit/SyncKit-Swift.h>
#import <CloudKit/CloudKit.h>

#define callBlockIfNotNil(block, ...) if (block){block(__VA_ARGS__);}

NSString * const QSCloudKitSynchronizerErrorDomain = @"QSCloudKitSynchronizerErrorDomain";
NSString * const QSCloudKitSynchronizerWillSynchronizeNotification = @"QSCloudKitSynchronizerWillSynchronizeNotification";
NSString * const QSCloudKitSynchronizerWillFetchChangesNotification = @"QSCloudKitSynchronizerWillFetchChangesNotification";
NSString * const QSCloudKitSynchronizerWillUploadChangesNotification = @"QSCloudKitSynchronizerWillUploadChangesNotification";
NSString * const QSCloudKitSynchronizerDidSynchronizeNotification = @"QSCloudKitSynchronizerDidSynchronizeNotification";
NSString * const QSCloudKitSynchronizerDidFailToSynchronizeNotification = @"QSCloudKitSynchronizerDidFailToSynchronizeNotification";
NSString * const QSCloudKitSynchronizerErrorKey = @"QSCloudKitSynchronizerErrorKey";

static const NSInteger QSDefaultBatchSize = 200;
NSString * const QSCloudKitDeviceUUIDKey = @"QSCloudKitDeviceUUIDKey";
NSString * const QSCloudKitModelCompatibilityVersionKey = @"QSCloudKitModelCompatibilityVersionKey";

typedef NS_ENUM(NSInteger, QSSynchronizerSyncStep)
{
    QSSynchronizerSyncStepPrepareForImport,
    QSSynchronizerSyncStepRestoreServerToken,
    QSSynchronizerSyncStepFetchChanges,
    QSSynchronizerSyncStepMergeChanges,
    QSSynchronizerSyncStepUpdateServerToken,
    QSSynchronizerSyncStepUploadChanges
};

@interface QSCloudKitSynchronizer ()

@property (nonatomic, readwrite, copy) NSString *identifier;

@property (nonatomic, readwrite, copy) NSString *containerIdentifier;
@property (nonatomic, strong) CKServerChangeToken *serverChangeToken;
@property (nonatomic, strong) NSMutableDictionary *activeZoneTokens;
@property (nonatomic, readwrite, assign) BOOL usesSharedDatabase;

@property (nonatomic, strong) CKDatabase *database;
@property (atomic, readwrite, assign, getter=isSyncing) BOOL syncing;

@property (nonatomic, assign) BOOL batchSizeWasParsedFromError;

@property (nonatomic, strong, readwrite) NSDictionary *modelAdapterDictionary;
@property (nonatomic, readwrite, strong) NSString *deviceIdentifier;

@property (nonatomic, assign) BOOL cancelSync;

@property (nonatomic, copy) void(^completion)(NSError *error);
@property (nonatomic, weak) NSOperation *currentOperation;

@property (nonatomic, readwrite, strong) dispatch_queue_t dispatchQueue;
@property (nonatomic, strong) NSOperationQueue *operationQueue;

@property (nonatomic, readwrite, strong) id<QSKeyValueStore> keyValueStore;
@property (nonatomic, readwrite, strong) id<QSCloudKitSynchronizerAdapterProvider> adapterProvider;
@property (nonatomic, assign) NSInteger maxBatchSize;

@property (nonatomic, strong) NSProgress *syncProgress;
@property (nonatomic, strong) NSProgress *uploadChangesProgress;

@end

@implementation QSCloudKitSynchronizer

- (instancetype)initWithIdentifier:(NSString *)identifier containerIdentifier:(NSString *)containerIdentifier database:(CKDatabase *)database adapterProvider:(id<QSCloudKitSynchronizerAdapterProvider>)adapterProvider
{
    return [self _initWithIdentifier:identifier containerIdentifier:containerIdentifier database:database adapterProvider:adapterProvider keyValueStore:[NSUserDefaults standardUserDefaults]];
}

- (instancetype)initWithIdentifier:(NSString *)identifier containerIdentifier:(NSString *)containerIdentifier database:(CKDatabase *)database adapterProvider:(id<QSCloudKitSynchronizerAdapterProvider>)adapterProvider keyValueStore:(id<QSKeyValueStore>)keyValueStore
{
    return [self _initWithIdentifier:identifier containerIdentifier:containerIdentifier database:database adapterProvider:adapterProvider keyValueStore:keyValueStore];
}

- (instancetype)_initWithIdentifier:(NSString *)identifier containerIdentifier:(NSString *)containerIdentifier database:(CKDatabase *)database adapterProvider:(id<QSCloudKitSynchronizerAdapterProvider>)adapterProvider keyValueStore:(id<QSKeyValueStore>)keyValueStore
{
    self = [super init];
    if (self) {
        self.identifier = identifier;
        self.adapterProvider = adapterProvider;
        self.keyValueStore = keyValueStore;
        self.containerIdentifier = containerIdentifier;
        self.modelAdapterDictionary = @{};
        
        self.maxBatchSize = QSDefaultBatchSize;
        self.batchSize = self.maxBatchSize;
        self.compatibilityVersion = 0;
        self.syncMode = QSCloudKitSynchronizeModeSync;
        self.database = database;
        
        [QSBackupDetection runBackupDetectionWithCompletion:^(QSBackupDetectionResult result, NSError *error) {
            if (result == QSBackupDetectionResultRestoredFromBackup) {
                [self clearDeviceIdentifier];
            }
        }];
        
        self.dispatchQueue = dispatch_queue_create("QSCloudKitSynchronizer", 0);
        self.operationQueue = [[NSOperationQueue alloc] init];
    }
    return self;
}

- (NSString *)deviceIdentifier
{
    if (!_deviceIdentifier) {
        
        _deviceIdentifier = [self getStoredDeviceUUID];
        if (!_deviceIdentifier) {
            NSUUID *UUID = [NSUUID UUID];
            _deviceIdentifier = [UUID UUIDString];
            [self storeDeviceUUID:_deviceIdentifier];
        }
    }
    return _deviceIdentifier;
}

- (void)clearDeviceIdentifier
{
    [self storeDeviceUUID:nil];
}

#pragma mark - Public

+ (NSArray<NSString *> *)synchronizerMetadataKeys
{
    static NSArray *metadataKeys = nil;
    static dispatch_once_t onceToken;
    dispatch_once(&onceToken, ^{
        metadataKeys = @[QSCloudKitDeviceUUIDKey, QSCloudKitModelCompatibilityVersionKey];
    });
    return metadataKeys;
}

- (NSProgress *)synchronizeWithCompletion:(void(^)(NSError *error))completion
{
    DLogInfo(@"<= completion = %d", completion == NULL? NO: YES);
    if (self.isSyncing) {
        callBlockIfNotNil(completion, [NSError errorWithDomain:QSCloudKitSynchronizerErrorDomain code:QSCloudKitSynchronizerErrorAlreadySyncing userInfo:nil]);
        return nil;
    }
    else if (self.completion != NULL)
    {
        // if previous completion is not nil, that means it is not being called yet and sync is not finished.
        DLog(@"previous completion is not finished yet");
        callBlockIfNotNil(completion, [NSError errorWithDomain:QSCloudKitSynchronizerErrorDomain code:QSCloudKitSynchronizerErrorAlreadySyncing userInfo:nil]);
        return nil;
    }
    
    self.syncProgress = [NSProgress progressWithTotalUnitCount:[self totalSyncProgressUnitCount]];
//    self.syncProgress.localizedDescription = NSLS_COMMON_SYNCHRONIZING;
    
    DLog(@"QSCloudKitSynchronizer >> Initiating synchronization");
    self.cancelSync = NO;
    self.syncing = YES;

    self.completion = completion;
    [self performSynchronization];
    
    return self.syncProgress;
}

- (void)cancelSynchronization
{
#if defined(DEBUG)
    DLog(@"cancel synchronization: %@", [NSThread callStackSymbols]);
#else
    DLogInfo(@"cancel synchronization");
#endif
    if (self.isSyncing) {
        self.cancelSync = YES;
        [self.currentOperation cancel];
    }
}

static void * MNCurrentOperationObservenceContext = &MNCurrentOperationObservenceContext;

- (void)setCurrentOperation:(CKOperation *)currentOperation
{
    _currentOperation = currentOperation;
    
    if (_currentOperation)
    {
        [_currentOperation addObserver:self forKeyPath:@"isFinished" options:NSKeyValueObservingOptionNew context:MNCurrentOperationObservenceContext];
        [_currentOperation addObserver:self forKeyPath:@"isCancelled" options:NSKeyValueObservingOptionNew context:MNCurrentOperationObservenceContext];
    }
}

- (void)observeValueForKeyPath:(NSString *)keyPath ofObject:(id)object change:(NSDictionary<NSKeyValueChangeKey,id> *)change context:(void *)context
{
    if (context == MNCurrentOperationObservenceContext)
    {
        DLogInfo(@"operation [%@] didChange [%@] - %@", object, keyPath, [object valueForKeyPath:keyPath]);
    }
    else
    {
        [super observeValueForKeyPath:keyPath ofObject:object change:change context:context];
    }
}

- (void)eraseLocalMetadata
{
    [self storeDatabaseToken:nil];
    [self clearAllStoredSubscriptionIDs];
    [self storeDeviceUUID:nil];
    
    for (id<QSModelAdapter> modelAdapter in [self.modelAdapters copy]) {
        [modelAdapter deleteChangeTracking];
        [self removeModelAdapter:modelAdapter];
    }
}

- (void)deleteRecordZoneForModelAdapter:(id<QSModelAdapter>)modelAdapter withCompletion:(void(^)(NSError *error))completion
{
    [self.database deleteRecordZoneWithID:modelAdapter.recordZoneID completionHandler:^(CKRecordZoneID * _Nullable zoneID, NSError * _Nullable error) {
        if (!error) {
            DLog(@"QSCloudKitSynchronizer >> Deleted zone: %@", zoneID);
        } else {
            DLog(@"QSCloudKitSynchronizer >> Error: %@", error);
        }
        callBlockIfNotNil(completion, error);
    }];
}

- (NSArray<id<QSModelAdapter> > *)modelAdapters
{
    return [self.modelAdapterDictionary allValues];
}

- (void)addModelAdapter:(id<QSModelAdapter>)modelAdapter
{
    NSMutableDictionary *updatedManagers = [self.modelAdapterDictionary mutableCopy];
    updatedManagers[modelAdapter.recordZoneID] = modelAdapter;
    self.modelAdapterDictionary = [updatedManagers copy];
}

- (void)removeModelAdapter:(id<QSModelAdapter>)modelAdapter
{
    NSMutableDictionary *updatedManagers = [self.modelAdapterDictionary mutableCopy];
    [updatedManagers removeObjectForKey:modelAdapter.recordZoneID];
    self.modelAdapterDictionary = [updatedManagers copy];
}

#pragma mark - Sync

- (void)performSynchronization
{
    dispatch_async(self.dispatchQueue, ^{
        dispatch_async(dispatch_get_main_queue(), ^{
            [[NSNotificationCenter defaultCenter] postNotificationName:QSCloudKitSynchronizerWillSynchronizeNotification object:self];
        });
        DLog(@"QSCloudKitSynchronizer >> performSynchronization.will prepareForImport");
        NSProgress *prepareForImportProgress = [NSProgress progressWithTotalUnitCount:1];
        [self.syncProgress addChild:prepareForImportProgress withPendingUnitCount:[self syncProgressUnitCountForStep:QSSynchronizerSyncStepPrepareForImport]];
        
        for (id<QSModelAdapter> modelAdapter in self.modelAdapters) {
            [modelAdapter prepareForImport];
        }
        
        [prepareForImportProgress setCompletedUnitCount:prepareForImportProgress.totalUnitCount];
        
        DLog(@"QSCloudKitSynchronizer >> performSynchronization. will synchronizationFetchChanges");
        [self synchronizationFetchChanges];
        DLog(@"QSCloudKitSynchronizer >> performSynchronization. did synchronizationFetchChanges");
    });
}

#pragma mark - 1) Fetch changes

- (void)synchronizationFetchChanges
{
    if (self.cancelSync) {
        [self finishSynchronizationWithError:[self cancelError]];
    } else {
        dispatch_async(dispatch_get_main_queue(), ^{
            [[NSNotificationCenter defaultCenter] postNotificationName:QSCloudKitSynchronizerWillFetchChangesNotification object:self];
        });
        DLog(@"QDCloudKitSynchronizer >> synchronizationFetchChanges");
        NSProgress *fetchChangesProgress = [NSProgress progressWithTotalUnitCount:1];
        [self.syncProgress addChild:fetchChangesProgress withPendingUnitCount:[self syncProgressUnitCountForStep:QSSynchronizerSyncStepFetchChanges]];
        __weak QSCloudKitSynchronizer *weakSelf = self;
        [self fetchDatabaseChangesWithCompletion:^(CKServerChangeToken *databaseToken, NSError *error) {
            if (error) {
                [weakSelf finishSynchronizationWithError:error];
            } else {
                [fetchChangesProgress setCompletedUnitCount:fetchChangesProgress.totalUnitCount];
                self.serverChangeToken = databaseToken;
                if (self.syncMode == QSCloudKitSynchronizeModeSync) {
                    [self synchronizationUploadChanges];
                } else {
                    [self finishSynchronizationWithError:nil];
                }
            }
        }];
    }
}

- (void)fetchDatabaseChangesWithCompletion:(void(^)(CKServerChangeToken *databaseToken, NSError *error))completion
{
    QSFetchDatabaseChangesOperation *operation = [[QSFetchDatabaseChangesOperation alloc] initWithDatabase:self.database
                                                                                             databaseToken:self.serverChangeToken
                                                                                                completion:^(CKServerChangeToken * _Nullable databaseToken, NSArray<CKRecordZoneID *> * _Nonnull changedZoneIDs, NSArray<CKRecordZoneID *> * _Nonnull deletedZoneIDs) {
        dispatch_async(self.dispatchQueue, ^{
            [self notifyProviderForDeletedZoneIDs:deletedZoneIDs];
            
            NSArray *zoneIDsToFetch = [self loadAdaptersAndTokensForZoneIDs:changedZoneIDs];
            if (zoneIDsToFetch.count) {
                
                [self fetchZoneChanges:zoneIDsToFetch withCompletion:^(NSError *error) {
                    if (error) {
                        [self finishSynchronizationWithError:error];
                    } else {
                        [self synchronizationMergeChangesWithCompletion:^(NSError *error) {
                            [self resetActiveTokens];
                            callBlockIfNotNil(completion, databaseToken, error);
                        }];
                    }
                }];
            } else {
                [self resetActiveTokens];
                callBlockIfNotNil(completion, databaseToken, nil);
            }
            
        });
                                                                                                    
    }];
    
    [self runOperation:operation];
}

- (NSArray<CKRecordZoneID *> *)loadAdaptersAndTokensForZoneIDs:(NSArray<CKRecordZoneID *> *)zoneIDs
{
    NSMutableArray *filteredZoneIDs = [NSMutableArray array];
    self.activeZoneTokens = [NSMutableDictionary dictionary];
    
    for (CKRecordZoneID *zoneID in zoneIDs) {
        id<QSModelAdapter> modelAdapter = self.modelAdapterDictionary[zoneID];
        if (!modelAdapter) {
            id<QSModelAdapter> newModelAdapter = [self.adapterProvider cloudKitSynchronizer:self modelAdapterForRecordZoneID:zoneID];
            if (newModelAdapter) {
                modelAdapter = newModelAdapter;
                NSMutableDictionary *updatedManagers = [self.modelAdapterDictionary mutableCopy];
                updatedManagers[zoneID] = newModelAdapter;
                [newModelAdapter prepareForImport];
                self.modelAdapterDictionary = [updatedManagers copy];
            }
        }
        if (modelAdapter) {
            [filteredZoneIDs addObject:zoneID];
            self.activeZoneTokens[zoneID] = [modelAdapter serverChangeToken];
        }
    }
    
    return [filteredZoneIDs copy];
}

- (void)resetActiveTokens
{
    self.activeZoneTokens = [NSMutableDictionary dictionary];
}

- (void)fetchZoneChanges:(NSArray *)zoneIDs withCompletion:(void(^)(NSError *error))completion
{
    void (^completionBlock)(NSDictionary<CKRecordZoneID *,QSFetchZoneChangesOperationZoneResult *> * _Nonnull zoneResults) = ^(NSDictionary<CKRecordZoneID *,QSFetchZoneChangesOperationZoneResult *> * _Nonnull zoneResults) {
       
        dispatch_async(self.dispatchQueue, ^{
            NSMutableArray *pendingZones = [NSMutableArray array];
            __block NSError *error = nil;
            [zoneResults enumerateKeysAndObjectsUsingBlock:^(CKRecordZoneID * _Nonnull zoneID, QSFetchZoneChangesOperationZoneResult * _Nonnull zoneResult, BOOL * _Nonnull stop) {
                
                id<QSModelAdapter> modelAdapter = self.modelAdapterDictionary[zoneID];
                if (zoneResult.error) {
                    error = zoneResult.error;
                    *stop = YES;
                } else {
                    DLog(@"QSCloudKitSynchronizer >> Downloaded %ld changed records >> from zone %@", (unsigned long)zoneResult.downloadedRecords.count, zoneID);
                    DLog(@"QSCloudKitSynchronizer >> Downloaded %ld deleted record IDs >> from zone %@", (unsigned long)zoneResult.deletedRecordIDs.count, zoneID);
                    self.activeZoneTokens[zoneID] = zoneResult.serverChangeToken;
                    [modelAdapter saveChangesInRecords:zoneResult.downloadedRecords];
                    [modelAdapter deleteRecordsWithIDs:zoneResult.deletedRecordIDs];
                    __block BOOL anyCkShareWasDeleted = NO;
                    [zoneResult.deletedRecordIDs enumerateObjectsUsingBlock:^(CKRecordID * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
                        if ([self isCKShareRecordID:obj])
                        {
                            anyCkShareWasDeleted = YES;
                            *stop = YES;
                        }
                    }];
                    if (anyCkShareWasDeleted)
                    {
                        [self handleCKShareDeletionInZone:zoneID];
                    }
                    if (zoneResult.moreComing) {
                        [pendingZones addObject:zoneID];
                    }
                }
            }];
            
            if (pendingZones.count && !error) {
                [self fetchZoneChanges:pendingZones withCompletion:completion];
            } else {
                callBlockIfNotNil(completion, error);
            }
        });
    };
    
    QSFetchZoneChangesOperation *operation = [[QSFetchZoneChangesOperation alloc] initWithDatabase:self.database
                                                                                           zoneIDs:zoneIDs
                                                                                  zoneChangeTokens:[self.activeZoneTokens copy]
                                                                                      modelVersion:self.compatibilityVersion
                                                                            ignoreDeviceIdentifier:nil
                                                                                       desiredKeys:nil
                                                                                        completion:completionBlock];
    [self runOperation:operation];
}

#pragma mark - 2) Merge changes

- (void)synchronizationMergeChangesWithCompletion:(void(^)(NSError *error))completion
{
    if (self.cancelSync) {
        [self finishSynchronizationWithError:[self cancelError]];
    } else {
        DLog(@"QSCloudKitSynchronizer >> synchronizarionMergeChanges");
        NSProgress *mergeChangesProgress = [NSProgress progressWithTotalUnitCount:100];
        [self.syncProgress addChild:mergeChangesProgress withPendingUnitCount:[self syncProgressUnitCountForStep:QSSynchronizerSyncStepMergeChanges]];
        NSMutableSet *modelAdapters = [NSMutableSet set];
        for (CKRecordZoneID *zoneID in self.activeZoneTokens.allKeys) {
            [modelAdapters addObject:self.modelAdapterDictionary[zoneID]];
        }
        [self mergeChanges:modelAdapters completion:^(NSError *error) {
            mergeChangesProgress.completedUnitCount = mergeChangesProgress.totalUnitCount;
            callBlockIfNotNil(completion, error);
        }];
    }
}

- (void)mergeChanges:(NSSet *)modelAdapters completion:(void(^)(NSError *error))completion
{
    id<QSModelAdapter> modelAdapter = [modelAdapters anyObject];
    if (!modelAdapter) {
        callBlockIfNotNil(completion, nil);
    } else {
        __weak QSCloudKitSynchronizer *weakSelf = self;
        [modelAdapter persistImportedChangesWithCompletion:^(NSError * _Nullable error) {
            NSMutableSet *pendingModelAdapters = [modelAdapters mutableCopy];
            [pendingModelAdapters removeObject:modelAdapter];
            
            if (!error) {
                [modelAdapter saveToken:self.activeZoneTokens[modelAdapter.recordZoneID]];
            }
            
            if (error) {
                callBlockIfNotNil(completion, error);
            } else {
                [weakSelf mergeChanges:[pendingModelAdapters copy] completion:completion];
            }
        }];
    }
}

#pragma mark - 3) Upload changes

- (void)synchronizationUploadChanges
{
    if (self.cancelSync) {
        [self finishSynchronizationWithError:[self cancelError]];
    } else {
        dispatch_async(dispatch_get_main_queue(), ^{
            [[NSNotificationCenter defaultCenter] postNotificationName:QSCloudKitSynchronizerWillUploadChangesNotification object:self];
        });
        self.uploadChangesProgress = [NSProgress progressWithTotalUnitCount:100];
        [self.syncProgress addChild:self.uploadChangesProgress withPendingUnitCount:[self syncProgressUnitCountForStep:QSSynchronizerSyncStepUploadChanges]];
        DLogInfo(@"QSCloudKitSynchronizer >> will upload new and updated objects: count n/a");
        [self uploadChangesWithCompletion:^(NSError *error) {

            if (error) {
                if ([self isServerRecordChangedError:error]) {
                    [self synchronizationFetchChanges];
                } else {
                    [self finishSynchronizationWithError:error];
                }
            } else {
                self.uploadChangesProgress.completedUnitCount = self.uploadChangesProgress.totalUnitCount;
                self.uploadChangesProgress = nil;
                [self synchronizationUpdateServerTokens];
            }
        }];
    }
}

- (void)uploadChangesWithCompletion:(void(^)(NSError *error))completion
{
    if (self.cancelSync) {
        [self finishSynchronizationWithError:[self cancelError]];
    } else {
        DLog(@"QSCloudKitSynchronizer >> uploadEntitiesForModelAdapterSet");
        [self uploadEntitiesForModelAdapterSet:[NSSet setWithArray:self.modelAdapters] completion:^(NSError *error) {
            if (error) {
                callBlockIfNotNil(completion, error);
            } else {
                [self uploadDeletionsWithCompletion:completion];
            }
        }];
    }
}

- (void)uploadDeletionsWithCompletion:(void(^)(NSError *error))completion
{
    if (self.cancelSync) {
        [self finishSynchronizationWithError:[self cancelError]];
    } else {
        DLog(@"QSCloudKitSynchronizer >> uploadDeletionsWithCompletion");
        [self removeDeletedEntitiesFromModelAdapters:[NSSet setWithArray:self.modelAdapters] completion:^(NSError *error) {
            self.uploadChangesProgress.completedUnitCount = self.uploadChangesProgress.totalUnitCount;
            callBlockIfNotNil(completion, error);
        }];
    }
}

- (void)uploadEntitiesForModelAdapterSet:(NSSet *)modelAdapters completion:(void(^)(NSError *error))completion
{
    if (modelAdapters.count == 0) {
        callBlockIfNotNil(completion, nil);
    } else {
        DLogInfo(@"<= modelAdapters = %@", modelAdapters);
        __weak QSCloudKitSynchronizer *weakSelf = self;
        id<QSModelAdapter> modelAdapter = [modelAdapters anyObject];
        [self setupRecordZoneIfNeeded:modelAdapter completion:^(NSError *error) {
            if (error) {
                callBlockIfNotNil(completion, error);
            } else {
                
                [self uploadEntitiesForModelAdapter:modelAdapter withCompletion:^(NSError *error) {
                    
                    if (error) {
                        callBlockIfNotNil(completion, error);
                    } else {
                        NSMutableSet *pendingModelAdapters = [modelAdapters mutableCopy];
                        [pendingModelAdapters removeObject:modelAdapter];
                        [weakSelf uploadEntitiesForModelAdapterSet:pendingModelAdapters completion:completion];
                    }
                }];
            }
        }];
    }
}

- (void)uploadEntitiesForModelAdapter:(id<QSModelAdapter>)modelAdapter withCompletion:(void(^)(NSError *error))completion
{
    NSArray *records = [modelAdapter recordsToUploadWithLimit:self.batchSize];
    NSInteger recordCount = records.count;
    NSInteger requestedBatchSize = self.batchSize;
    DLogInfo(@"QSCloudKitSynchronizer >> Changes to upload during current operation: %ld for modelAdapter: %@", (long)recordCount, modelAdapter);
    if (recordCount == 0) {
        callBlockIfNotNil(completion, nil);
    } else {
        __weak QSCloudKitSynchronizer *weakSelf = self;
        //Add metadata: device UUID and model version
        [self addMetadataToRecords:records];
        //Now perform the operation
        CKModifyRecordsOperation *modifyRecordsOperation = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:records recordIDsToDelete:nil];
        __weak CKModifyRecordsOperation *weakModifyRecordsOperation = modifyRecordsOperation;
        [self.class configureCKOperation:modifyRecordsOperation];

        NSMutableArray *recordsToSave = [NSMutableArray array];
        NSMutableArray *recordsWithUnknownItemError = [NSMutableArray array];
        modifyRecordsOperation.perRecordCompletionBlock = ^(CKRecord *record, NSError *error) {
            dispatch_async(self.dispatchQueue, ^{
                if (error.code == CKErrorServerRecordChanged) {
                    //Update local data with server
                    CKRecord *aRecord = error.userInfo[CKRecordChangedErrorServerRecordKey];
                    if (aRecord) {
                        [recordsToSave addObject:aRecord];
                    }
                }
                else if ([self isUnknownItemError:error])
                {
                    //Unknown record to cloudkit. upload is at new
                    [recordsWithUnknownItemError addObject:record];
                }
            });
        };
        
        __block BOOL modifyRecordsCompletionBlockWasCalled = NO;
        modifyRecordsOperation.modifyRecordsCompletionBlock = ^(NSArray <CKRecord *> *savedRecords, NSArray <CKRecordID *> *deletedRecordIDs, NSError *operationError) {
            modifyRecordsCompletionBlockWasCalled = YES;
            dispatch_async(self.dispatchQueue, ^{
                
                DLog(@"QSCloudKitSynchronizer >> modifyRecordsOperation.modifyRecordsCompletionBlock");
                if (self.cancelSync || weakModifyRecordsOperation.isCancelled)
                {
                    callBlockIfNotNil(completion, [NSError errorWithDomain:QSCloudKitSynchronizerErrorDomain code:0 userInfo:@{ QSCloudKitSynchronizerErrorKey : @"Synchronization was cancelled"}]);
                    return;
                }
                [modelAdapter saveChangesInRecords:recordsToSave];
                [modelAdapter handleRecordsWithUnknownItemError:recordsWithUnknownItemError];
                if (self.cancelSync || weakModifyRecordsOperation.isCancelled)
                {
                    callBlockIfNotNil(completion, [NSError errorWithDomain:QSCloudKitSynchronizerErrorDomain code:0 userInfo:@{ QSCloudKitSynchronizerErrorKey : @"Synchronization was cancelled"}]);
                    return;
                }
                if (!operationError) {
                    
                    if (self.batchSize < self.maxBatchSize) {
                        self.batchSize += 5;
                    }
                    
                    [modelAdapter didUploadRecords:savedRecords];
                    
                    DLog(@"QSCloudKitSynchronizer >> Uploaded %ld records, batchSize: %ld", (unsigned long)savedRecords.count, (long)self.batchSize);
                    
                    if (recordCount >= requestedBatchSize) {
                        [weakSelf uploadEntitiesForModelAdapter:modelAdapter withCompletion:completion];
                    } else {
                        callBlockIfNotNil(completion, operationError);
                    }
                } else {
                    if ([self isLimitExceededError:operationError]) {
                        [self updateBatchSizeFromError:operationError];
                    }
                    DLog(@"QSCloudKitSynchronizer >> new batchSize: %ld", (long)self.batchSize);
                    
                    callBlockIfNotNil(completion, operationError);
                }
            });
        };
        
        modifyRecordsOperation.completionBlock = ^{
            if (!modifyRecordsCompletionBlockWasCalled && completion != NULL)
            {
                // modifyRecordsCompletionBlock was not called
                NSError *newError = [QSCloudKitSynchronizer completionBlockWasNotCalledError];
                callBlockIfNotNil(completion, newError);
            }
        };
        
        self.currentOperation = modifyRecordsOperation;
        NSInteger totalRequestsCount = self.batchSize != 0 ? (NSUInteger)self.uploadChangesProgress.totalUnitCount / self.batchSize : 0;
        NSInteger currentRequestIndex = self.batchSize != 0 ? (NSUInteger)(self.uploadChangesProgress.completedUnitCount + records.count) / self.batchSize : 0;
        DLogInfo(@"QSCloudKitSynchronizer >> will upload %ld changes. %ld / %ld requests, batchSuze: %ld", (long)recordCount, (long)currentRequestIndex, (long)totalRequestsCount, (long)self.batchSize);
        [self.database addOperation:modifyRecordsOperation];
    }
}

- (void)removeDeletedEntitiesFromModelAdapters:(NSSet *)modelAdapters completion:(void(^)(NSError *error))completion
{
    if (modelAdapters.count == 0) {
        callBlockIfNotNil(completion, nil);
    } else {
        __weak QSCloudKitSynchronizer *weakSelf = self;
        id<QSModelAdapter> modelAdapter = [modelAdapters anyObject];
        [self removeDeletedEntitiesFromModelAdapter:modelAdapter completion:^(NSError *error) {
            if (error) {
                callBlockIfNotNil(completion, error);
            } else {
                NSMutableSet *pendingModelAdapters = [modelAdapters mutableCopy];
                [pendingModelAdapters removeObject:modelAdapter];
                [weakSelf removeDeletedEntitiesFromModelAdapters:pendingModelAdapters completion:completion];
            }
        }];
    }
}

- (void)removeDeletedEntitiesFromModelAdapter:(id<QSModelAdapter>)modelAdapter completion:(void(^)(NSError *error))completion
{
    if (modelAdapter)
    {
        [modelAdapter recordIDsMarkedForDeletionWithLimit:self.batchSize completion:^(NSArray<CKRecordID *> *recordIDs) {
            NSInteger recordCount = recordIDs.count;
            
            if (recordCount == 0) {
                callBlockIfNotNil(completion, nil);
            } else {
                //Now perform the operation
                CKModifyRecordsOperation *modifyRecordsOperation = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:nil recordIDsToDelete:recordIDs];
                [self.class configureCKOperation:modifyRecordsOperation];
                __block BOOL modifyRecordsOperationCompletionBlockWasCalled = NO;
                modifyRecordsOperation.modifyRecordsCompletionBlock = ^(NSArray <CKRecord *> *savedRecords, NSArray <CKRecordID *> *deletedRecordIDs, NSError *operationError) {
                    modifyRecordsOperationCompletionBlockWasCalled = YES;
                    dispatch_async(self.dispatchQueue, ^{
                        DLog(@"QSCloudKitSynchronizer >> Deleted %ld records, batchSize: %ld", (unsigned long)deletedRecordIDs.count, (long)self.batchSize);
                        
                        BOOL batchSizeChanged = NO;
                        if (operationError.code == CKErrorLimitExceeded) {
                            [self updateBatchSizeFromError:operationError];
                            batchSizeChanged = YES;
                        } else if (self.batchSize < self.maxBatchSize) {
                            if (!self.batchSizeWasParsedFromError)
                            {
                                self.batchSize += 5;
                                batchSizeChanged = YES;
                            }
                        }
                        if (batchSizeChanged)
                        {
                            DLog(@"QSCloudKitSynchronizer >> new batchSize: %ld", (long)self.batchSize);
                        }
                        
                        [modelAdapter didDeleteRecordIDs:deletedRecordIDs];
                        
                        callBlockIfNotNil(completion,operationError);
                    });
                };
                
                modifyRecordsOperation.completionBlock = ^{
                    if (!modifyRecordsOperationCompletionBlockWasCalled && completion != NULL)
                    {
                        //modifyRecordsOperationCompletionBlock was not called
                        NSError *newError = [QSCloudKitSynchronizer completionBlockWasNotCalledError];
                        callBlockIfNotNil(completion, newError);
                    }
                };
                
                self.currentOperation = modifyRecordsOperation;
                DLog(@"QSCloudKitSynchronizer >> modifyRecordsOperation (deletion) will add to db: %@", self.database);
                [self.database addOperation:modifyRecordsOperation];
            }
        }];
    }
    else
    {
        callBlockIfNotNil(completion,nil);
    }
}

#pragma mark - 4) Update tokens

- (void)synchronizationUpdateServerTokens
{
    DLog(@"QSCloudKitSynchronizer >> synchronizationUpdateServerTokens");
    NSProgress *updateServerTokenProgress = [NSProgress progressWithTotalUnitCount:1];
    [self.syncProgress addChild:updateServerTokenProgress withPendingUnitCount:[self syncProgressUnitCountForStep:QSSynchronizerSyncStepUpdateServerToken]];
    void (^completionBlock)(CKServerChangeToken * _Nullable, NSArray<CKRecordZoneID *> * _Nonnull, NSArray<CKRecordZoneID *> * _Nonnull) = ^(CKServerChangeToken * _Nullable databaseToken, NSArray<CKRecordZoneID *> * _Nonnull changedZoneIDs, NSArray<CKRecordZoneID *> * _Nonnull deletedZoneIDs) {
        
        [self notifyProviderForDeletedZoneIDs:deletedZoneIDs];
        if (changedZoneIDs.count) {
            [self updateServerTokenForRecordZones:changedZoneIDs withCompletion:^(BOOL needToFetchFullChanges) {
                updateServerTokenProgress.completedUnitCount = updateServerTokenProgress.totalUnitCount;
                if (needToFetchFullChanges) {
                    //There were changes before we finished, repeat process again
                    [self performSynchronization];
                } else {
                    self.serverChangeToken = databaseToken;
                    [self finishSynchronizationWithError:nil];
                }
            }];
        } else {
            [self finishSynchronizationWithError:nil];
        }
    };
    
    QSFetchDatabaseChangesOperation *operation = [[QSFetchDatabaseChangesOperation alloc] initWithDatabase:self.database
                                                                                             databaseToken:self.serverChangeToken
                                                                                                completion:completionBlock];
    
    [self runOperation:operation];
}

- (void)updateServerTokenForRecordZones:(NSArray<CKRecordZoneID *> *)zoneIDs withCompletion:(void(^)(BOOL needToFetchFullChanges))completion
{
    DLogInfo(@"QSCloudKitSynchronizer >> updateServerTokenForRecordZoneIDs: %@", zoneIDs);
    void(^completionBlock)(NSDictionary<CKRecordZoneID *,QSFetchZoneChangesOperationZoneResult *> * _Nonnull) = ^(NSDictionary<CKRecordZoneID *,QSFetchZoneChangesOperationZoneResult *> * _Nonnull zoneResults) {
        dispatch_async(self.dispatchQueue, ^{
            NSMutableArray *pendingZones = [NSMutableArray array];
            __block BOOL needsToRefetch = NO;
            [zoneResults enumerateKeysAndObjectsUsingBlock:^(CKRecordZoneID * _Nonnull zoneID, QSFetchZoneChangesOperationZoneResult * _Nonnull result, BOOL * _Nonnull stop) {
                id<QSModelAdapter> modelAdapter = self.modelAdapterDictionary[zoneID];
                if (result.downloadedRecords.count || result.deletedRecordIDs.count) {
                    needsToRefetch = YES;
                } else {
                    [modelAdapter saveToken:result.serverChangeToken];
                }
                if (result.moreComing) {
                    [pendingZones addObject:zoneID];
                }
            }];
            
            if (pendingZones.count && !needsToRefetch) {
                [self updateServerTokenForRecordZones:pendingZones withCompletion:completion];
            } else {
                callBlockIfNotNil(completion, needsToRefetch);
            }
        });
    };
    
    QSFetchZoneChangesOperation *operation = [[QSFetchZoneChangesOperation alloc] initWithDatabase:self.database
                                                                                           zoneIDs:zoneIDs
                                                                                  zoneChangeTokens:[self.activeZoneTokens copy]
                                                                                      modelVersion:self.compatibilityVersion
                                                                            ignoreDeviceIdentifier:self.deviceIdentifier
                                                                                       desiredKeys:@[@"recordID", QSCloudKitDeviceUUIDKey]
                                                                                        completion:completionBlock];
    
    [self runOperation:operation];
}

#pragma mark - 5) Finish

- (void)finishSynchronizationWithError:(NSError *)error
{
    self.syncProgress.completedUnitCount = self.syncProgress.totalUnitCount;
    self.syncing = NO;
    self.cancelSync = NO;
    
    [self resetActiveTokens];
    
    if ([self isChangeTokenExpiredError:error])
    {
        self.serverChangeToken = nil;
    }

    for (id<QSModelAdapter> modelAdapter in self.modelAdapters) {
        [modelAdapter didFinishImportWithError:error];
    }
    dispatch_async(dispatch_get_main_queue(), ^{
        if (error) {
            [[NSNotificationCenter defaultCenter] postNotificationName:QSCloudKitSynchronizerDidFailToSynchronizeNotification
                                                                object:self
                                                              userInfo:@{QSCloudKitSynchronizerErrorKey : error}];
        } else {
            [[NSNotificationCenter defaultCenter] postNotificationName:QSCloudKitSynchronizerDidSynchronizeNotification object:self];
        }
        
        void(^aCompletion)(NSError *error) = self.completion;
        self.completion = nil;
        callBlockIfNotNil(aCompletion, error);
    });
    
    DLog(@"QSCloudKitSynchronizer >> Finishing synchronization");
}

- (BOOL)isChangeTokenExpiredError:(NSError *)error
{
    if (error.code == CKErrorPartialFailure) {
        NSDictionary *errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey];
        for (NSError *anError in [errorsByItemID allValues]) {
            if (anError.code == CKErrorChangeTokenExpired) {
                return YES;
            }
        }
    }
    
    return error.code == CKErrorChangeTokenExpired;
}

#pragma mark - Utilities

@synthesize serverChangeToken = _serverChangeToken;

- (CKServerChangeToken *)serverChangeToken
{
    if (!_serverChangeToken) {
        _serverChangeToken = [self getStoredDatabaseToken];
    }
    return _serverChangeToken;
}

- (void)setServerChangeToken:(CKServerChangeToken *)serverChangeToken
{
    _serverChangeToken = serverChangeToken;
    [self storeDatabaseToken:serverChangeToken];
}

- (NSError *)cancelError {
    return [NSError errorWithDomain:QSCloudKitSynchronizerErrorDomain code:QSCloudKitSynchronizerErrorCancelled userInfo:@{QSCloudKitSynchronizerErrorKey: @"Synchronization was canceled"}];
}

- (void)runOperation:(QSCloudKitSynchronizerOperation *)operation
{
    operation.errorHandler = ^(QSCloudKitSynchronizerOperation * _Nonnull operation, NSError * _Nonnull error) {
        if ([self isZoneNotFoundPartialError:error] && self.modelAdapters.count > 0)
        {
            // sometimes after wipe zone is being returned as changed and synckit does not handle it properly. This should fix such very rare? cases
            [self setupRecordZoneIfNeeded:self.modelAdapters.firstObject completion:^(NSError *setupZoneError) {
                [self finishSynchronizationWithError:error];
            }];
        }
        else
        {
            [self finishSynchronizationWithError:error];
        }
    };
    self.currentOperation = operation;
    [self.operationQueue addOperation:operation];
}

- (BOOL)isZoneNotFoundPartialError:(NSError *)error
{
    if (error.code == CKErrorPartialFailure)
    {
        NSDictionary *errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey];
        for (NSError *anError in [errorsByItemID allValues])
        {
            if (anError.code == CKErrorZoneNotFound || anError.code == CKErrorUserDeletedZone)
            {
                return YES;
            }
        }
    }
    return NO;
}

- (void)notifyProviderForDeletedZoneIDs:(NSArray<CKRecordZoneID *> *)zoneIDs
{
    for (CKRecordZoneID *zoneID in zoneIDs) {
        [self.adapterProvider cloudKitSynchronizer:self zoneWasDeletedWithZoneID:zoneID];
    }
}

- (BOOL)isLimitExceededError:(NSError *)error
{
    if (error.code == CKErrorPartialFailure) {
        NSDictionary *errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey];
        for (NSError *error in [errorsByItemID allValues]) {
            if (error.code == CKErrorLimitExceeded) {
                return YES;
            }
        }
    }
    
    return error.code == CKErrorLimitExceeded;
}

- (BOOL)isServerRecordChangedError:(NSError *)error
{
    if (error.code == CKErrorPartialFailure) {
        NSDictionary *errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey];
        for (NSError *error in [errorsByItemID allValues]) {
            if (error.code == CKErrorServerRecordChanged) {
                return YES;
            }
        }
    }
    
    return error.code == CKErrorServerRecordChanged;
}

- (BOOL)isUnknownItemError:(NSError *)error
{
    if (error.code == CKErrorPartialFailure)
    {
        NSDictionary *errorsByItemID = error.userInfo[CKPartialErrorsByItemIDKey];
        for (NSError *anError in [errorsByItemID allValues])
        {
            if (anError.code == CKErrorUnknownItem)
            {
                return YES;
            }
        }
    }
    return error.code == CKErrorUnknownItem;
}

#pragma mark - RecordZone setup

- (BOOL)needsZoneSetup:(id<QSModelAdapter>)modelAdapter
{
    return modelAdapter.serverChangeToken == nil;
}

- (void)setupRecordZoneIfNeeded:(id<QSModelAdapter>)modelAdapter completion:(void(^)(NSError *error))completion
{
    if ([self needsZoneSetup:modelAdapter]) {
        [self setupRecordZone:modelAdapter.recordZoneID withCompletion:^(NSError *error) {
            callBlockIfNotNil(completion, error);
        }];
    } else {
        completion(nil);
    }
}

- (void)setupRecordZone:(CKRecordZoneID *)zoneID withCompletion:(void(^)(NSError *error))completionBlock
{
    DLog(@"will setupRecordZone");
    [self.database fetchRecordZoneWithID:zoneID completionHandler:^(CKRecordZone * _Nullable zone, NSError * _Nullable error) {
        
        if (zone) {
            DLog(@"zone fetched");
            callBlockIfNotNil(completionBlock, error);
        } else if (error.code  == CKErrorZoneNotFound || error.code == CKErrorUserDeletedZone) {

            DLog(@"will create new zone");
            CKRecordZone *newZone = [[CKRecordZone alloc] initWithZoneID:zoneID];
            [self.database saveRecordZone:newZone completionHandler:^(CKRecordZone * _Nullable zone, NSError * _Nullable error) {
                if (!error && zone) {
                    DLog(@"QSCloudKitSynchronizer >> Created custom record zone: %@", zone);
                }
                callBlockIfNotNil(completionBlock, error);
            }];
            
        } else {
            callBlockIfNotNil(completionBlock, error);
        }
        
    }];
}

#pragma mark - private

- (void)updateBatchSizeFromError:(NSError *)error
{
    NSString *errorMessage = error.localizedDescription;
    NSInteger newBatchSize = self.batchSize / 2;
    if ([errorMessage hasSuffix:@")"])
    {
        errorMessage = [errorMessage substringToIndex:errorMessage.length - 1];
        NSRange lastBracketRange = [errorMessage rangeOfString:@"(" options:NSBackwardsSearch];
        if (lastBracketRange.length > 0)
        {
            NSString *parsedBatchString = [errorMessage substringFromIndex:lastBracketRange.location + lastBracketRange.length];
            newBatchSize = [parsedBatchString integerValue];
            if (newBatchSize > 0 && newBatchSize < self.batchSize)
            {
                // batchsuze was parsed and it is less than the current batch size
                self.batchSizeWasParsedFromError = YES;
            }
            DLog(@"parsedBatchString: %@, batchSize: %ld", parsedBatchString, (long)newBatchSize);
        }
    }
    if (newBatchSize >= self.batchSize)
    {
        newBatchSize = self.batchSize / 2;
    }
    self.batchSize = newBatchSize;
}

+ (void)configureCKOperation:(CKOperation *)operation
{
    operation.qualityOfService = NSQualityOfServiceUserInitiated; //to reduce timeoutInterval time
}

- (NSInteger)totalSyncProgressUnitCount
{
    __block NSInteger totalSyncProgressUnitCount = 0;
    NSDictionary *syncProgressUnitCountByStep = [self syncProgressUnitCountByStep];
    [syncProgressUnitCountByStep enumerateKeysAndObjectsUsingBlock:^(id  _Nonnull key, id  _Nonnull unitCount, BOOL * _Nonnull stop) {
        totalSyncProgressUnitCount += [unitCount integerValue];
    }];
    return totalSyncProgressUnitCount;
}

- (NSInteger)syncProgressUnitCountForStep:(QSSynchronizerSyncStep)syncStep
{
    return [[self syncProgressUnitCountByStep][@(syncStep)] integerValue];
}

- (NSDictionary *)syncProgressUnitCountByStep
{
    NSDictionary *syncProgressUnitCountByStep = @{
                                                  @(QSSynchronizerSyncStepPrepareForImport) : @(1),
                                                  @(QSSynchronizerSyncStepRestoreServerToken) : @(1),
                                                  @(QSSynchronizerSyncStepFetchChanges) : @(1),
                                                  @(QSSynchronizerSyncStepMergeChanges) : @(1),
                                                  @(QSSynchronizerSyncStepUpdateServerToken) : @(1),
                                                  @(QSSynchronizerSyncStepUploadChanges) : @(1)
                                                  };
    return syncProgressUnitCountByStep;
}

- (void)handleCKShare:(CKShare *)deletedShare deletionInZone:(CKRecordZoneID *)zoneID completion:(void (^ _Nullable)(NSError * _Nullable))completion
{
    if (self.database.databaseScope == CKDatabaseScopePrivate)
    {
        // private CKShare was deleted;
        // stop sharing extra data share to users, that is not participating in any "normal" share.
        NSPredicate *predicate = [NSPredicate predicateWithFormat:@"QSCloudKitDeviceUUIDKey != 'sdfarg'"];// [NSPredicate predicateWithFormat:@"TRUEPREDICATE"] - does not work;
        CKQuery *query = [[CKQuery alloc] initWithRecordType:CKRecordTypeShare predicate:predicate];
        CKQueryOperation *operation = [[CKQueryOperation alloc] initWithQuery:query];
        operation.desiredKeys = @[];
        operation.zoneID = zoneID;
        NSMutableArray <CKShare *>*existingShares = [NSMutableArray array];
        __block BOOL queryCompletionBlockWasCalled = NO;
        
        operation.recordFetchedBlock = ^(CKRecord *record) {
            if ([record isKindOfClass:[CKShare class]])
            {
                CKShare *aShare = (CKShare *)record;
                [existingShares addObject:aShare];
            }
        };
        
        operation.queryCompletionBlock = ^(CKQueryCursor *cursor, NSError *error) {
            queryCompletionBlockWasCalled = YES;
        };
        
        operation.completionBlock = ^{
            __block CKShare *extraDataShare = nil;
            __block NSMutableSet *userRecordNamesThatAcceptedAnyShare = [NSMutableSet set];
            DLogInfo(@"existingShares: %@", existingShares);
            [existingShares enumerateObjectsUsingBlock:^(CKShare * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
                if (![self isExtraSharedDataShare:obj])
                {
                    [obj.participants enumerateObjectsUsingBlock:^(CKShareParticipant * _Nonnull aParticipant, NSUInteger idxParticipant, BOOL * _Nonnull stopParticipant) {
                        if (aParticipant.acceptanceStatus == CKShareParticipantAcceptanceStatusAccepted)
                        {
                            [userRecordNamesThatAcceptedAnyShare addObject:aParticipant.userIdentity.userRecordID.recordName];
                        }
                    }];
                }
                else
                {
                    extraDataShare = obj;
                }
            }];
            
            if (extraDataShare.participants.count > 0)
            {
                NSMutableSet *participantsToRemoveFromExtraSharedData = [NSMutableSet set];
                NSMutableArray *updatedParticipants = [NSMutableArray array];
                [extraDataShare.participants enumerateObjectsUsingBlock:^(CKShareParticipant * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
                    if (![userRecordNamesThatAcceptedAnyShare containsObject:obj.userIdentity.userRecordID.recordName])
                    {
                        [participantsToRemoveFromExtraSharedData addObject:obj];
                    }
                    else
                    {
                        [updatedParticipants addObject:obj];
                    }
                }];
                DLogInfo(@"participants to remove from extra shared data: %ld", (long)participantsToRemoveFromExtraSharedData.count);
                if (participantsToRemoveFromExtraSharedData.count > 0)
                {
                    __block BOOL shouldStopSharingExtraData = NO;
                    [participantsToRemoveFromExtraSharedData enumerateObjectsUsingBlock:^(CKShareParticipant * _Nonnull obj, BOOL * _Nonnull stop) {
                        if ([obj isEqual:extraDataShare.owner])
                        {
                            // current user does not have any accounts shared (currently we does not stop sharing, because otherwise it will take longer for user to share account)
//                            shouldStopSharingExtraData = YES;
                        }
                        else
                        {
                            [extraDataShare removeParticipant:obj];
                        }
                    }];
                    
                    if (shouldStopSharingExtraData)
                    {
                        DLogInfo(@"shouldStopSharingExtraData");  //(currently we does not stop sharing, because otherwise it will take longer for user to share account)
                    }
                    else
                    {
                        DLogInfo(@"(not) shouldStopSharingExtraData");
                        [self saveChangesForShare:extraDataShare withCompletion:^(CKShare * _Nullable savedRecord, NSError * _Nonnull saveChangesForShareError) {
                            DLogInfo(@"did update shared data share: %@", savedRecord);
                            if (saveChangesForShareError)
                            {
                                DLogInfo(@"saveChangesForShareError: %@", saveChangesForShareError);
                            }
                            __block BOOL hasShareLocally = NO;
                            [self.modelAdapters enumerateObjectsUsingBlock:^(id <QSModelAdapter> obj, NSUInteger idx, BOOL *stop){
                                if ([obj hasRecordID:deletedShare.recordID])
                                {
                                    hasShareLocally = YES;
                                    *stop = YES;
                                }
                            }];
                            if (hasShareLocally)
                            {
                                dispatch_async(dispatch_get_main_queue(), ^{
                                    // fix (workaround) share not disappearing locally when removed. If syncing  data after some delay - it disappears.
                                    [self performSelector:@selector(synchronizeWithCompletion:) withObject:completion afterDelay:2.0];
                                });
                            }
                            else
                            {
                                if (completion != NULL)
                                {
                                    completion(saveChangesForShareError);
                                }
                            }
                        }];
                        return;
                    }
                }
                __block BOOL hasShareLocally = NO;
                [self.modelAdapters enumerateObjectsUsingBlock:^(id <QSModelAdapter> obj, NSUInteger idx, BOOL *stop){
                    if ([obj hasRecordID:deletedShare.recordID])
                    {
                        hasShareLocally = YES;
                        *stop = YES;
                    }
                }];
                if (hasShareLocally)
                {
                    dispatch_async(dispatch_get_main_queue(), ^{
                        // fix (workaround) share not disappearing locally when removed. If syncing  data after some delay - it disappears.
                        [self performSelector:@selector(synchronizeWithCompletion:) withObject:completion afterDelay:2.0];
                    });
                }
                else
                {
                    if (completion != NULL)
                    {
                        completion(nil);
                    }
                }
            }
            else
            {
                if (completion != NULL)
                {
                    completion(nil);
                }
            }
        };
        [self.database addOperation:operation];
    }
    else if (self.database.databaseScope == CKDatabaseScopeShared)
    {
        // other owner's CKShare was deleted
        DLogInfo(@"handle CKShare deletion in shared db");
        // delete shared data parent object's share? (if no other account from that user is being accepted)
        
        NSPredicate *predicate = [NSPredicate predicateWithFormat:@"QSCloudKitDeviceUUIDKey != 'sdfarg'"];// [NSPredicate predicateWithFormat:@"TRUEPREDICATE"] - does not work;
        CKQuery *query = [[CKQuery alloc] initWithRecordType:CKRecordTypeShare predicate:predicate];
        CKQueryOperation *queryOperation = [[CKQueryOperation alloc] initWithQuery:query];
        queryOperation.desiredKeys = @[];
        queryOperation.zoneID = zoneID;
        NSMutableArray <CKShare *>*existingShares = [NSMutableArray array];
        __block BOOL queryCompletionBlockWasCalled = NO;
        
        queryOperation.recordFetchedBlock = ^(CKRecord *record) {
            if ([record isKindOfClass:[CKShare class]])
            {
                CKShare *aShare = (CKShare *)record;
                [existingShares addObject:aShare];
            }
        };
        
        queryOperation.queryCompletionBlock = ^(CKQueryCursor *cursor, NSError *error) {
            queryCompletionBlockWasCalled = YES;
        };
        
        queryOperation.completionBlock = ^{
            __block CKShare *extraDataShare = nil;
            __block BOOL hasAcceptedAnyAccountFromRemovedShareOwner = NO;
            [existingShares enumerateObjectsUsingBlock:^(CKShare * _Nonnull obj, NSUInteger idx, BOOL * _Nonnull stop) {
                
                if (![self isExtraSharedDataShare:obj])
                {
                    [obj.participants enumerateObjectsUsingBlock:^(CKShareParticipant * _Nonnull aParticipant, NSUInteger idxParticipant, BOOL * _Nonnull stopParticipant) {
                        if (obj.currentUserParticipant == aParticipant)
                        {
                            *stopParticipant = YES;
                            hasAcceptedAnyAccountFromRemovedShareOwner = YES;
                        }
                    }];
                }
                else
                {
                    extraDataShare = obj;
                }
                
            }];
            
            if (extraDataShare && !hasAcceptedAnyAccountFromRemovedShareOwner)
            {
                DLogInfo(@"extraDataShare && !hasAcceptedAnyAccount");
                CKModifyRecordsOperation *deleteShareOperation = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:@[] recordIDsToDelete:@[extraDataShare.recordID]];
                deleteShareOperation.queuePriority = NSOperationQueuePriorityHigh;
                deleteShareOperation.qualityOfService = NSQualityOfServiceUserInitiated;
                deleteShareOperation.modifyRecordsCompletionBlock = ^void(NSArray<CKRecord *> * _Nullable savedRecords, NSArray<CKRecordID *> * _Nullable deletedRecordIDs, NSError * _Nullable deleteShareOperationError) {
                    if (deleteShareOperationError)
                    {
                        DLogInfo(@"deleteShareOperationError: %@", deleteShareOperationError);
                    }
                    if ([deletedRecordIDs containsObject:extraDataShare.recordID])
                    {
                        DLogInfo(@"extra data share removed successfully remotelly");
                        // maybe synchronizeSharedData is not needed here (because if extra data share for some owner was deleted, then likely all shares from it were deleted in the shared db and therefore zone was deleted. So, data update likely was done during delete zone notification), but just in case.
                    }
                    [self synchronizeWithCompletion:^(NSError *syncSharedDataError) {
                        if (syncSharedDataError)
                        {
                            DLogInfo(@"syncSharedDataError: %@", syncSharedDataError);
                        }
                        __block BOOL hasShareLocally = NO;
                        [self.modelAdapters enumerateObjectsUsingBlock:^(id <QSModelAdapter> obj, NSUInteger idx, BOOL *stop){
                            if ([obj hasRecordID:deletedShare.recordID])
                            {
                                hasShareLocally = YES;
                                *stop = YES;
                            }
                        }];
                        if (hasShareLocally)
                        {
                            dispatch_async(dispatch_get_main_queue(), ^{
                                // fix (workaround) share not disappearing locally when removed. If syncing shared data after some delay - it disappears.
                                [self performSelector:@selector(synchronizeWithCompletion:) withObject:NULL afterDelay:3.0];
                            });
                        }
                        if (completion != NULL)
                        {
                            completion(syncSharedDataError);
                        }
                    }];
                };
                [self.database addOperation:deleteShareOperation];
            }
            else
            {
                [self synchronizeWithCompletion:^(NSError *syncSharedDataError) {
                    DLogInfo(@"syncSharedDataError: %@",syncSharedDataError);
                    __block BOOL hasShareLocally = NO;
                    [self.modelAdapters enumerateObjectsUsingBlock:^(id <QSModelAdapter> obj, NSUInteger idx, BOOL *stop){
                        if ([obj hasRecordID:deletedShare.recordID])
                        {
                            hasShareLocally = YES;
                            *stop = YES;
                        }
                    }];
                    if (hasShareLocally)
                    {
                        dispatch_async(dispatch_get_main_queue(), ^{
                            // fix (workaround) share not disappearing locally when removed. If syncing shared data after some delay - it disappears.
                            [self performSelector:@selector(synchronizeWithCompletion:) withObject:NULL afterDelay:3.0];
                        });
                    }
                    if (completion != NULL)
                    {
                        completion(syncSharedDataError);
                    }
                }];
            }
        };
        [self.database addOperation:queryOperation];
    }
}

- (void)handleCKShareDeletionInZone:(CKRecordZoneID *)zoneID
{
    [self handleCKShare:nil deletionInZone:(CKRecordZoneID *)zoneID completion:NULL];
}

- (BOOL)isExtraSharedDataShare:(CKShare *)share
{
    BOOL isExtraSharedDataShare = NO;
    if (share.publicPermission == CKShareParticipantPermissionReadWrite)
    {
        // extra shared data share has readwrite permission, while account, budget? - private
        isExtraSharedDataShare = YES;
    }
    return isExtraSharedDataShare;
}

- (BOOL)isCKShareRecordID:(CKRecordID *)recordID
{
    return [recordID.recordName hasPrefix:@"Share-"];
}

+ (NSError *)completionBlockWasNotCalledError
{
    return [NSError errorWithDomain:QSCloudKitSynchronizerErrorDomain code:QSCloudKitSynchronizerErrorCompletionBlockNotCalled userInfo:@{ NSLocalizedDescriptionKey : @"QS Operation completion block not being called" }];
}

- (void)saveChangesForShare:(CKShare *)share withCompletion:(void(^)(CKShare * _Nullable savedRecord, NSError *error))completion
{
    CKRecord *recordCopy = share.copy;
    CKModifyRecordsOperation *operation = [[CKModifyRecordsOperation alloc] initWithRecordsToSave:@[recordCopy] recordIDsToDelete:nil];
    operation.queuePriority = NSOperationQueuePriorityHigh;
    operation.qualityOfService = NSQualityOfServiceUserInitiated;
    operation.modifyRecordsCompletionBlock = ^void(NSArray<CKRecord *> * _Nullable savedRecords, NSArray<CKRecordID *> * _Nullable deletedRecordIDs, NSError * _Nullable operationError) {
        
        __block CKShare *savedShare = nil;
        [savedRecords enumerateObjectsUsingBlock:^(CKRecord * _Nonnull obj, NSUInteger idxEnumerateSavedRecords, BOOL * _Nonnull stopEnumerateSavedRecords) {
            if ([obj isKindOfClass:[CKShare class]])
            {
                savedShare = (CKShare *)obj;
                *stopEnumerateSavedRecords = YES;
            }
        }];
        
        DLogInfo(@"Saved share %@, with error %@", savedShare, operationError);
        dispatch_async(dispatch_get_main_queue(), ^{
            
            if (completion != NULL)
            {
                completion(savedShare, operationError);
            }
            
        });
        
    };
    
    [self.database addOperation:operation];
}

@end
