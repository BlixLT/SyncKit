//
//  QSCoreDataAdapter.h
//  Pods
//
//  Created by Manuel Entrena on 10/07/2016.
//
//

#import <Foundation/Foundation.h>
#import "QSModelAdapter.h"
#import "QSCoreDataStack.h"
#import <CoreData/CoreData.h>
#import <CloudKit/CloudKit.h>

@class QSCoreDataAdapter;

/**
 *  An object implementing `QSCoreDataAdapterDelegate` is responsible for saving the target managed object context at the request of the `QSCoreDataAdapter` in order to persist any downloaded changes.
 */
@protocol QSCoreDataAdapterDelegate <NSObject>

/**
 *  Asks the delegate to save the target managed object context before attempting to merge downloaded changes.
 *
 *  @param coreDataAdapter The `QSCoreDataAdapter` requesting the delegate to save.
 *  @param completion    Block to be called once the managed object context has been saved.
 */
- (void)coreDataAdapterRequestsContextSave:(nullable QSCoreDataAdapter *)coreDataAdapter completion:(void(^_Nullable)(NSError * _Nullable error))completion;

/**
 *  Tells the delegate to merge downloaded changes into the managed object context. First, the `importContext` must be saved by using `performBlock`. Then, the target managed object context must be saved to persist those changes and the completion block must be called to finalize the synchronization process.
 *
 *  @param coreDataAdapter The `QSCoreDataAdapter` that is providing the changes.
 *  @param importContext `NSManagedObjectContext` containing all downloaded changes. This context has the target context as its parent context.
 *  @param completion    Block to be called once contexts have been saved.
 */
- (void)coreDataAdapter:(QSCoreDataAdapter *_Nullable)coreDataAdapter didImportChanges:(NSManagedObjectContext *_Nullable)importContext completion:(void(^_Nullable)(NSError * _Nullable error))completion;

@end

/**
 *  An object implementing `QSCoreDataAdapterConflictResolutionDelegate` is responsible for deciding which changes to keep for a given object, when there is a conflict between local changes and changes downloaded from iCloud.
 */
@protocol QSCoreDataAdapterConflictResolutionDelegate <NSObject>

/**
 *  Asks the delegate to resolve conflicts for a managed object. The delegate is expected to examine the change dictionary and optionally apply any of those changes to the managed object.
 *
 *  @param coreDataAdapter    The `QSCoreDataAdapter` that is providing the changes.
 *  @param changeDictionary Dictionary containing keys and values with changes for the managed object. Values could be [NSNull null] to represent a nil value.
 *  @param object           The `NSManagedObject` that has changed on iCloud.
 */
- (void)coreDataAdapter:(QSCoreDataAdapter *_Nullable)coreDataAdapter gotChanges:(NSDictionary *_Nullable)changeDictionary forObject:(NSManagedObject *_Nullable)object;

@end


/**
 *  A `QSCoreDataAdapter` object implements the `QSModelAdapter` protocol to manage changes in a Core Data model.
 */
@interface QSCoreDataAdapter : NSObject <QSModelAdapter>

/**
 *  The `NSManagedObjectModel` used by the change manager to keep track of changes.
 *
 *  @return The model.
 */
+ (NSManagedObjectModel *_Nonnull)persistenceModel;

/**
 *  Initializes a new `QSCoreDataAdapter`.
 *
 *  @param stack         Core Data stack that will be used by the change manager to persist tracking information.
 *  @param targetContext `NSManagedObjectContext` that will be tracked to detect changes and merge new ones.
 *  @param zoneID        Identifier of the `CKRecordZone` that will contain all data on iCloud.
 *  @param delegate      Delegate that will take care of saving the target context when needed.
 *
 *  @return Initialized core data change manager.
 */
- (instancetype _Nullable )initWithPersistenceStack:(QSCoreDataStack *_Nullable)stack targetContext:(NSManagedObjectContext *_Nullable)targetContext recordZoneID:(CKRecordZoneID *_Nullable)zoneID delegate:(id<QSCoreDataAdapterDelegate>_Nullable)delegate;

/**
 *  The target context that will be tracked. (read-only)
 */
@property (nonatomic, readonly) NSManagedObjectContext * _Nullable targetContext;
/**
 *  Delegate. (read-only)
 */
@property (nonatomic, weak, readonly) id<QSCoreDataAdapterDelegate> _Nullable delegate;

@property (nonatomic, weak) id<QSCoreDataAdapterConflictResolutionDelegate> _Nullable conflictDelegate;
/**
 *  Identifier of record zone that will contain data. (read-only)
 */
@property (nonatomic, readonly) CKRecordZoneID * _Nonnull recordZoneID;
/**
 *  Core Data stack used for tracking information. (read-only)
 */
@property (nonatomic, readonly) QSCoreDataStack * _Nullable stack;

/**
 *  Merge policy to be used in case of conflicts. Default value is `QSModelAdapterMergePolicyServer`
 */
@property (nonatomic, assign) QSModelAdapterMergePolicy mergePolicy;

/**
 *  If this is true data fields will be uploaded as data, instead of using CKAsset properties on the records.
 */
@property (nonatomic, assign) BOOL forceDataTypeInsteadOfAsset;

- (void)updateTrackingForObjectsWithPrimaryKey;

#pragma mark - JSAdditions

- (void)countEntitiesToBeUploadedWithCompletion:(void(^_Nonnull)(NSInteger entitiesCount))completion;

@end
