//
//  QSCloudKitSynchronizerTests.m
//  QSCloudKitSynchronizer
//
//  Created by Manuel Entrena on 23/07/2016.
//  Copyright © 2016 Manuel. All rights reserved.
//

#import <XCTest/XCTest.h>
#import <SyncKit/QSCloudKitSynchronizer.h>
#import <OCMock/OCMock.h>
#import <CloudKit/CloudKit.h>
#import "QSMockChangeManager.h"
#import "QSMockDatabase.h"
#import "QSObject.h"

@interface QSCloudKitSynchronizerTests : XCTestCase

@property (nonatomic, strong) QSCloudKitSynchronizer *synchronizer;
@property (nonatomic, strong) QSMockDatabase *mockDatabase;
@property (nonatomic, strong) QSMockChangeManager *mockChangeManager;
@property (nonatomic, strong) CKRecordZoneID *recordZoneID;
@property (nonatomic, strong) id mockContainer;

@end

@implementation QSCloudKitSynchronizerTests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
    
    //Pass our custom database
    self.mockDatabase = [QSMockDatabase new];
    
    self.mockContainer = OCMClassMock([CKContainer class]);
    OCMStub([self.mockContainer privateCloudDatabase]).andReturn(self.mockDatabase);
    OCMStub([self.mockContainer containerWithIdentifier:[OCMArg any]]).andReturn(self.mockContainer);
    
    self.mockChangeManager = [[QSMockChangeManager alloc] init];
    
    self.recordZoneID = [[CKRecordZoneID alloc] initWithZoneName:@"zone" ownerName:@"owner"];
    
    self.synchronizer = [[QSCloudKitSynchronizer alloc] initWithContainerIdentifier:@"any" recordZoneID:self.recordZoneID changeManager:self.mockChangeManager];
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)clearAllUserDefaults
{
    NSString *appDomain = [[NSBundle mainBundle] bundleIdentifier];
    [[NSUserDefaults standardUserDefaults] removePersistentDomainForName:appDomain];
    [[NSUserDefaults standardUserDefaults] synchronize];
}

- (CKSubscription *)subscription
{
    CKSubscription *subscription;
    if (@available(iOS 10.0, macOS 10.12, watchOS 3.0, *)) {
        subscription = [[CKRecordZoneSubscription alloc] initWithZoneID:self.recordZoneID];
    } else {
        subscription = [[CKSubscription alloc] initWithZoneID:self.recordZoneID options:0];
    }
    
    CKNotificationInfo *notificationInfo = [[CKNotificationInfo alloc] init];
    notificationInfo.shouldSendContentAvailable = YES;
    subscription.notificationInfo = notificationInfo;
    return subscription;
}

- (void)testSynchronize_twoObjectsToUpload_uploadsThem
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1], [[QSObject alloc] initWithIdentifier:@"2" number:@2]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:objects];
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssert(self.mockDatabase.receivedRecords.count == 2);
}

- (void)testSynchronize_oneObjectToDelete_deletesObject
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1], [[QSObject alloc] initWithIdentifier:@"2" number:@2]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForDeletion:@[[objects lastObject]]];
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssert(self.mockDatabase.deletedRecordIDs.count == 1);
    XCTAssert(self.mockChangeManager.objects.count == 1);
}

- (void)testSynchronize_oneObjectToFetch_downloadsObject
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1], [[QSObject alloc] initWithIdentifier:@"2" number:@2]];
    self.mockChangeManager.objects = objects;
    
    QSObject *object = [[QSObject alloc] initWithIdentifier:@"3" number:@3];
    
    self.mockDatabase.readyToFetchRecords = @[[object record]];
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssert(self.mockChangeManager.objects.count == 3);
}

- (void)testSynchronize_objectsToUploadAndDeleteAndFetch_UpdatesAll
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1],
                         [[QSObject alloc] initWithIdentifier:@"2" number:@2],
                         [[QSObject alloc] initWithIdentifier:@"3" number:@3],
                         [[QSObject alloc] initWithIdentifier:@"4" number:@4]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:@[[objects objectAtIndex:0]]];
    [self.mockChangeManager markForDeletion:@[[objects lastObject]]];
    
    QSObject *object = [[QSObject alloc] initWithIdentifier:@"5" number:@5];
    
    self.mockDatabase.readyToFetchRecords = @[[object record]];
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssert(self.mockDatabase.deletedRecordIDs.count == 1);
    XCTAssert(self.mockDatabase.receivedRecords.count == 1);
    XCTAssert(self.mockChangeManager.objects.count == 4);
    QSObject *fifthObject = nil;
    for (QSObject *obj in self.mockChangeManager.objects) {
        if ([obj.identifier isEqualToString:@"5"]) {
            fifthObject = obj;
        }
    }
    XCTAssertNotNil(fifthObject);
}

- (void)testSynchronize_errorInFetch_endsWithError
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    
    NSError *error = [NSError errorWithDomain:@"error" code:0 userInfo:nil];
    self.mockDatabase.fetchError = error;
    
    __block NSError *receivedError = nil;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        receivedError = error;
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertEqualObjects(error, receivedError);
}

- (void)testSynchronize_errorInUpload_endsWithError
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    
    NSError *error = [NSError errorWithDomain:@"error" code:0 userInfo:nil];
    self.mockDatabase.uploadError = error;
    
    QSObject *object = [[QSObject alloc] initWithIdentifier:@"1" number:@1];
    self.mockChangeManager.objects = @[object];
    [self.mockChangeManager markForUpload:@[object]];
    
    __block NSError *receivedError = nil;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        receivedError = error;
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertEqualObjects(error, receivedError);
}

- (void)testSynchronize_limitExceededError_decreasesBatchSizeAndEndsWithError
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    
    NSInteger batchSize = self.synchronizer.batchSize;
    NSError *error = [NSError errorWithDomain:@"error"
                                         code:CKErrorLimitExceeded
                                     userInfo:nil];
    self.mockDatabase.uploadError = error;
    
    QSObject *object = [[QSObject alloc] initWithIdentifier:@"1" number:@1];
    self.mockChangeManager.objects = @[object];
    [self.mockChangeManager markForUpload:@[object]];
    
    __block NSError *receivedError = nil;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        receivedError = error;
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    NSInteger halfBatchSize = self.synchronizer.batchSize;
    
    XCTAssertEqualObjects(error, receivedError);
    XCTAssertEqual(halfBatchSize, batchSize / 2);
    
    
    /*
     Synchronize without error increases batch size
     */
    
    self.mockDatabase.uploadError = nil;
    self.mockChangeManager.objects = @[object];
    [self.mockChangeManager markForUpload:@[object]];
    XCTestExpectation *expectation2 = [self expectationWithDescription:@"Sync finished"];
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        receivedError = error;
        [expectation2 fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertNil(receivedError);
    XCTAssertTrue(self.synchronizer.batchSize > halfBatchSize);
}

- (void)testSynchronize_limitExceededErrorInPartialError_decreasesBatchSizeAndEndsWithError
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    
    NSInteger batchSize = self.synchronizer.batchSize;
    NSError *error = [NSError errorWithDomain:@"error"
                                         code:CKErrorPartialFailure
                                     userInfo:@{CKPartialErrorsByItemIDKey: @{@"itemID": [NSError errorWithDomain:@"error"
                                                                                                       code:CKErrorLimitExceeded
                                                                                                   userInfo:nil]}
                                                
                                                }
                      ];
    self.mockDatabase.uploadError = error;
    
    QSObject *object = [[QSObject alloc] initWithIdentifier:@"1" number:@1];
    self.mockChangeManager.objects = @[object];
    [self.mockChangeManager markForUpload:@[object]];
    
    __block NSError *receivedError = nil;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        receivedError = error;
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertEqualObjects(error, receivedError);
    XCTAssertEqual(self.synchronizer.batchSize, batchSize / 2);
}

- (void)testSynchronize_moreThanBatchSizeItems_performsMultipleUploads
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    
    NSMutableArray *objects = [NSMutableArray array];
    for (NSInteger i = 0; i < self.synchronizer.batchSize + 10; i++) {
        QSObject *object = [[QSObject alloc] initWithIdentifier:[NSString stringWithFormat:@"%ld", (long)i] number:@(i)];
        [objects addObject:object];
    }
    
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:objects];
    
    __block NSInteger operationCount = 0;
    self.mockDatabase.modifyRecordsOperationEnqueuedBlock = ^(CKModifyRecordsOperation *operation) {
        operationCount++;
    };
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertTrue(operationCount > 1);
}

- (void)testSynchronize_storesServerTokenAfterFetch
{
    //Make sure no token is carried over from another test
    [self.synchronizer eraseLocal];
    
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1], [[QSObject alloc] initWithIdentifier:@"2" number:@2]];
    self.mockChangeManager.objects = objects;
    
    
    __block NSString *firstToken = nil;
    __block NSString *lastToken = nil;
    __block BOOL firstCall = YES;
    
    self.mockDatabase.fetchRecordChangesOperationEnqueuedBlock = ^(CKFetchRecordChangesOperation *operation) {
        if (firstCall) {
            firstToken = (NSString *)operation.previousServerChangeToken;
            firstCall = NO;
        } else {
            lastToken = (NSString *)operation.previousServerChangeToken;
        }
    };
    
    self.mockDatabase.fetchRecordZoneChangesOperationEnqueuedBlock = ^(CKFetchRecordZoneChangesOperation *operation) {
        if (firstCall) {
            firstToken = (NSString *)operation.optionsByRecordZoneID[self.recordZoneID].previousServerChangeToken;
            firstCall = NO;
        } else {
            lastToken = (NSString *)operation.optionsByRecordZoneID[self.recordZoneID].previousServerChangeToken;
        }
    };
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertNil(firstToken);
    XCTAssertNotNil(lastToken);
}

- (void)testEraseLocal_deletesChangeManagerTracking
{
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1], [[QSObject alloc] initWithIdentifier:@"2" number:@2]];
    self.mockChangeManager.objects = objects;
    
    [self.synchronizer eraseLocal];
    
    XCTAssert(self.mockChangeManager.objects.count == 0);
}

- (void)testEraseRemote_deletesRecordZone
{
    __block BOOL called = NO;
    self.mockDatabase.deleteRecordZoneCalledBlock = ^(CKRecordZoneID *zoneID) {
        called = YES;
    };
    
    [self.synchronizer eraseRemoteAndLocalDataWithCompletion:^(NSError *error) {
        
    }];
    
    XCTAssert(called == YES);
}

- (void)testSubscribeForUpdateNotifications_savesToDatabase
{
    [self clearAllUserDefaults];
    
    XCTestExpectation *expectation = [self expectationWithDescription:@"Save subscription called"];
    
    __block BOOL called = NO;
    
    self.mockDatabase.saveSubscriptionCalledBlock = ^(CKSubscription *subscription) {
        called = YES;
        [expectation fulfill];
    };
    
    [self.synchronizer subscribeForUpdateNotificationsWithCompletion:nil];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];

    XCTAssertTrue(called);
}

- (void)testSubscribeForUpdateNotifications_existingSubscription_updatesSubscriptionID
{
    [self clearAllUserDefaults];
    
    CKSubscription *subscription = [self subscription];
    self.mockDatabase.subscriptions = @[subscription];
    
    XCTestExpectation *expectation = [self expectationWithDescription:@"Fetched subscriptions"];
    
    self.mockDatabase.fetchAllSubscriptionsCalledBlock = ^ {
        [expectation fulfill];
    };
    
    __block BOOL saveCalled = NO;
    self.mockDatabase.saveSubscriptionCalledBlock = ^(CKSubscription *subscription) {
        saveCalled = YES;
    };
    
    [self.synchronizer subscribeForUpdateNotificationsWithCompletion:nil];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertFalse(saveCalled);
    XCTAssertEqual(self.synchronizer.subscriptionID, subscription.subscriptionID);
}

- (void)testDeleteSubscription_deletesOnDatabase
{
    
    XCTestExpectation *saved = [self expectationWithDescription:@"saved"];
    [self.synchronizer subscribeForUpdateNotificationsWithCompletion:^(NSError *error) {
        [saved fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    
    XCTestExpectation *deleted = [self expectationWithDescription:@"Save subscription called"];
    __block BOOL called = NO;
    
    self.mockDatabase.deleteSubscriptionCalledBlock = ^(NSString *subscriptionID) {
        called = YES;
        [deleted fulfill];
    };
    
    [self.synchronizer deleteSubscriptionWithCompletion:nil];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertTrue(called);
}

- (void)testDeleteSubscription_noLocalSubscriptionButRemoteOne_deletesOnDatabase
{
    [self clearAllUserDefaults];
    
    CKSubscription *subscription = [self subscription];
    self.mockDatabase.subscriptions = @[subscription];
    
    XCTestExpectation *deleted = [self expectationWithDescription:@"Save subscription called"];
    
    __block NSString *deletedSubscriptionID = nil;
    self.mockDatabase.deleteSubscriptionCalledBlock = ^(NSString *subscriptionID) {
        deletedSubscriptionID = subscriptionID;
        [deleted fulfill];
    };
    
    [self.synchronizer deleteSubscriptionWithCompletion:nil];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    XCTAssertEqual(deletedSubscriptionID, subscription.subscriptionID);
}

- (void)testSynchronize_objectChanges_callsAllChangeManagerMethods
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1],
                         [[QSObject alloc] initWithIdentifier:@"2" number:@2],
                         [[QSObject alloc] initWithIdentifier:@"3" number:@3]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:@[[objects objectAtIndex:1]]];
    [self.mockChangeManager markForDeletion:@[[objects objectAtIndex:2]]];
    
    QSObject *object = [[QSObject alloc] initWithIdentifier:@"4" number:@4];
    
    self.mockDatabase.readyToFetchRecords = @[[object record]];
    self.mockDatabase.toDeleteRecordIDs = @[[(QSObject *)[objects firstObject] recordID]];
    
    id mock = OCMPartialMock(self.mockChangeManager);
    
    OCMExpect([mock prepareForImport]).andForwardToRealObject();
    OCMExpect([mock saveChangesInRecords:[OCMArg any]]).andForwardToRealObject();
    OCMExpect([mock deleteRecordsWithIDs:[OCMArg any]]).andForwardToRealObject();
    [[[[mock expect] ignoringNonObjectArgs] andForwardToRealObject] recordsToUploadWithLimit:1];
    OCMExpect([mock didUploadRecords:[OCMArg any]]).andForwardToRealObject();
    [[[[mock expect] ignoringNonObjectArgs] andForwardToRealObject] recordIDsMarkedForDeletionWithLimit:1];
    OCMExpect([mock didDeleteRecordIDs:[OCMArg any]]).andForwardToRealObject();
    OCMExpect([mock persistImportedChangesWithCompletion:[OCMArg any]]).andForwardToRealObject();
    OCMExpect([mock didFinishImportWithError:[OCMArg any]]).andForwardToRealObject();
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    OCMVerifyAll(mock);
    [mock stopMocking];
}

- (void)testSynchronize_newerModelVersion_cancelsSynchronizationWithError
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1],
                         [[QSObject alloc] initWithIdentifier:@"2" number:@2],
                         [[QSObject alloc] initWithIdentifier:@"3" number:@3]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:objects];
    
    id mock = OCMPartialMock(self.mockChangeManager);
    
    self.synchronizer.compatibilityVersion = 2;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    [self.synchronizer eraseLocal]; //For the test, to let the second synchronizer consider records uploaded from the same device
    
    QSCloudKitSynchronizer *synchronizer2 = [[QSCloudKitSynchronizer alloc] initWithContainerIdentifier:@"any" recordZoneID:[[CKRecordZoneID alloc] initWithZoneName:@"zone" ownerName:@"owner"] changeManager:self.mockChangeManager];
    
    self.mockDatabase.readyToFetchRecords = self.mockDatabase.receivedRecords;
    
    XCTestExpectation *expectation2 = [self expectationWithDescription:@"Sync finished"];
    
    //Now update compatibility version and fetch records
    synchronizer2.compatibilityVersion = 1;
    __block NSError *syncError = nil;
    
    [synchronizer2 synchronizeWithCompletion:^(NSError *error) {
        syncError = error;
        [expectation2 fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    XCTAssertTrue([syncError.domain isEqualToString:QSCloudKitSynchronizerErrorDomain]);
    XCTAssertTrue(syncError.code == QSCloudKitSynchronizerErrorHigherModelVersionFound);
    
    [mock stopMocking];
}

- (void)testSynchronize_usesModelVersion_synchronizesWithPreviousVersions
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1],
                         [[QSObject alloc] initWithIdentifier:@"2" number:@2],
                         [[QSObject alloc] initWithIdentifier:@"3" number:@3]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:objects];
    
    id mock = OCMPartialMock(self.mockChangeManager);
    
    self.synchronizer.compatibilityVersion = 1;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    self.mockDatabase.readyToFetchRecords = self.mockDatabase.receivedRecords;
    
    XCTestExpectation *expectation2 = [self expectationWithDescription:@"Sync finished"];
    
    //Now update compatibility version and fetch records
    self.synchronizer.compatibilityVersion = 2;
    __block NSError *syncError = nil;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        syncError = error;
        [expectation2 fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    XCTAssertNil(syncError);
    
    [mock stopMocking];
}

- (void)testSynchronize_downloadOnly_doesNotUploadChanges
{
    XCTestExpectation *expectation = [self expectationWithDescription:@"Sync finished"];
    NSArray *objects = @[[[QSObject alloc] initWithIdentifier:@"1" number:@1],
                         [[QSObject alloc] initWithIdentifier:@"2" number:@2],
                         [[QSObject alloc] initWithIdentifier:@"3" number:@3]];
    self.mockChangeManager.objects = objects;
    [self.mockChangeManager markForUpload:objects];
    
    QSObject *object1 = [[QSObject alloc] initWithIdentifier:@"4" number:@4];
    QSObject *object2 = [[QSObject alloc] initWithIdentifier:@"3" number:@5];
    
    self.mockDatabase.readyToFetchRecords = @[[object1 record], [object2 record]];
    
    self.synchronizer.syncMode = QSCloudKitSynchronizeModeDownload;
    
    [self.synchronizer synchronizeWithCompletion:^(NSError *error) {
        [expectation fulfill];
    }];
    
    [self waitForExpectationsWithTimeout:1 handler:nil];
    
    QSObject *updatedObject = nil;
    for (QSObject *object in self.mockChangeManager.objects) {
        if ([object.identifier isEqualToString:@"3"]) {
            updatedObject = object;
            break;
        }
    }
    
    XCTAssertTrue(self.mockDatabase.receivedRecords.count == 0);
    XCTAssertTrue(self.mockChangeManager.objects.count == 4);
    XCTAssertTrue([updatedObject.number isEqual:@5]);
}

- (void)testPerformanceExample {
    // This is an example of a performance test case.
    [self measureBlock:^{
        // Put the code you want to measure the time of here.
    }];
}

@end
